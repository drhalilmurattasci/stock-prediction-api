# Stock Market Analysis & Price Prediction Model API — Master Plan

> **Status:** Draft v1 · **Owner:** drhalilmurattasci@gmail.com
> A living planning document: full API feature catalog, engineering doctrine, not-to-do list, committed technology stack, architecture, and phased roadmap.

---

## Table of Contents
1. [Overview](#1-overview)
2. [Engineering & Product Doctrine](#2-engineering--product-doctrine)
3. [Not To Do (Anti-Patterns)](#3-not-to-do-anti-patterns)
4. [Technology Stack & Architecture](#4-technology-stack--architecture)
5. [API & Framework Feature Catalog](#5-api--framework-feature-catalog)
   - [5.1 Market & Fundamental Data APIs](#51-market--fundamental-data-apis)
   - [5.2 Modeling, Forecasting & Infra Frameworks](#52-modeling-forecasting--infra-frameworks)
6. [Phased Roadmap & Delivery Plan](#6-phased-roadmap--delivery-plan)
7. [Appendix: Source Selection Summary](#7-appendix-source-selection-summary)

---

## 1. Overview

Markets are not a slot machine you can read, and they are not a coin flip you can ignore — they are a near-efficient system where most public information is already reflected in the price. This product is built on that premise. The **Stock Market Analysis & Price Prediction Model API** is a REST/WebSocket service that ingests live and historical market data, computes structured analysis (trend, volatility, momentum, regime, correlation, and risk metrics), and serves **probabilistic price forecasts with explicit confidence intervals** — distributions and ranges with attached uncertainty, not point-estimate "this stock will hit $X" calls. The WebSocket layer streams updates as new data arrives; the REST layer serves on-demand analysis, backtested model output, and forecast snapshots.

The intended users are developers and quantitatively-minded teams who want a forecasting and analytics primitive they can build on top of: fintech app builders, quant researchers and students, portfolio dashboard and screener products, and internal trading-desk tooling. We are selling an *engine and an API*, not an end-user trading app — the consumer is a program or an analyst, and the output is structured data (JSON over REST, event streams over WebSocket) designed to be composed into someone else's product.

It is important to be honest about the two-layer reality of what we are building. The bottom layer — market data feeds, exchange/vendor pricing, historical bars, fundamentals, and reference data — is something we **build on, not build**. We integrate, normalize, cache, and rate-limit against third-party data sources (and inherit their licensing, latency, and accuracy constraints). The top layer — the analytics pipeline, feature engineering, the probabilistic forecasting models, calibration of confidence intervals, backtesting, and the API surface itself — is what we **build and own**. Conflating these two is the most common way projects like this overpromise; keeping them distinct keeps our scope, costs, and dependencies clear.

A blunt word on what this is and is not: **Markets are near-efficient, so no model — ours included — can reliably "predict" prices, and anyone claiming otherwise is selling a fantasy. We therefore ship calibrated *analysis and probabilistic forecasts with confidence intervals*, never point-accurate predictions, and never trading or investment advice.** Our success metric is forecast *calibration and well-quantified uncertainty* (do our 80% intervals contain the outcome ~80% of the time?), not the impossible goal of being "right" about tomorrow's close. Every response surfaces uncertainty as a first-class field, and the product carries a clear not-financial-advice posture throughout.

---

## 2. Engineering & Product Doctrine

#### Data Integrity

- **Serve only point-in-time data.** Every feature, label, and price must reflect exactly what was knowable at the timestamp it is attached to. Store an `as_of` timestamp on every record and reconstruct historical state by filtering on it — never by reading the latest value of a row that has since been revised.
- **Eliminate lookahead bias mechanically, not by inspection.** Lag every fundamental, estimate, and macro series by its real-world publication delay (e.g., quarterly earnings become available on the filing date, not the period-end date). Build a CI check that fails if any feature column has a non-null value before its `available_at` time.
- **Defend against survivorship bias.** Include delisted, merged, and bankrupt tickers in all historical universes and backtests. Source index constituents as of each rebalance date, not today's membership list.
- **Handle corporate actions explicitly and reversibly.** Store raw (unadjusted) prices plus a separate adjustment-factor series for splits, dividends, and spin-offs. Compute adjusted prices on read so a later correction to an action does not silently rewrite history. Document which adjustment convention each endpoint returns.
- **Treat data as immutable and versioned.** Vendor restatements get a new version with provenance, never an in-place overwrite. Any model or backtest result must be reproducible from a pinned data snapshot ID.

#### Modeling Discipline

- **Always benchmark against naive baselines first.** No model ships unless it beats random walk / last-value, moving-average, and ARIMA baselines on the same walk-forward splits and metrics. Report the baseline numbers alongside every result; "it predicts" is not a result.
- **Prefer probabilistic forecasts over point estimates.** Emit predictive distributions or intervals (quantiles, predicted volatility) and score them with proper scoring rules (pinball loss, CRPS, log-likelihood), not just MAE/RMSE on the mean.
- **Validate with walk-forward / expanding-window evaluation only.** No random k-fold shuffling of time series — it leaks the future. Use purged, embargoed splits so train and test windows never overlap across the prediction horizon.
- **Make every experiment reproducible.** Pin random seeds, data snapshot IDs, code commit, and environment. Log every run (params, metrics, artifacts) to an experiment tracker; a result that cannot be re-run from its logged config does not count.
- **Evaluate on economically meaningful metrics.** Report directional accuracy, calibration, and net-of-cost performance (slippage, fees, turnover), not just statistical error. A model with great RMSE and negative net return is a failed model.

#### API Design

- **Version every endpoint from day one.** Expose `/v1/...` and never make breaking changes within a version; introduce `/v2` instead. Publish a deprecation timeline and a `Sunset` header before retiring anything.
- **Make mutating and forecast requests idempotent.** Support an `Idempotency-Key` header so retries never double-charge or double-run a job. Identical inputs at the same data version must return identical, cacheable outputs.
- **Enforce rate limits and quotas per API key.** Return `429` with `Retry-After` and standard `RateLimit-*` headers. Apply tiered limits and backpressure so one client cannot starve others or blow the vendor budget.
- **Define typed, schema-validated contracts.** Publish an OpenAPI/JSON-Schema spec, validate every request and response against it, and reject malformed input with structured `4xx` errors. No untyped free-form JSON blobs.
- **Never leak raw vendor data.** Expose derived predictions, features, and aggregates — not verbatim licensed feeds. Strip vendor identifiers and field names that would let a client reconstruct the underlying subscription, and gate raw-data access behind explicit licensing checks.

#### Legal & Compliance

- **Honor redistribution licensing.** Confirm in writing that each data source permits the exact use (display, derived works, redistribution, caching) before it touches a customer-facing path. Track per-vendor redistribution rights in code/config and enforce them at the API boundary.
- **Attach "not investment advice" disclaimers everywhere.** Every prediction response, doc page, and dashboard must state outputs are informational only, not financial advice or a solicitation. Make the disclaimer part of the response payload, not just the website footer.
- **Comply with vendor Terms of Service.** Respect crawl rules, access frequency, field-level usage restrictions, and attribution requirements. Keep an auditable record of which ToS version was accepted and review it before changing how data is consumed.
- **Keep an audit trail.** Log who accessed which data and which model produced which forecast, with timestamps, so we can answer regulatory or vendor inquiries and demonstrate compliance.

#### Security

- **Manage secrets in a vault, never in code or config.** API keys, DB credentials, and vendor tokens live in a secrets manager and are injected at runtime. No secrets in source control, logs, error messages, or client-visible responses; rotate them on a schedule and on any suspected leak.
- **Authenticate every request with scoped API keys.** No anonymous access to prediction or data endpoints. Support key rotation and instant revocation, and bind keys to specific scopes, environments, and rate tiers.
- **Apply least privilege everywhere.** Each service, key, and human gets the minimum permissions needed — read-only where possible, separate prod/staging credentials, network egress restricted to required vendors. Default deny; grant explicitly.
- **Encrypt in transit and at rest.** Enforce TLS on all endpoints and encrypt stored data and backups. Validate and sanitize all inputs to prevent injection.

#### Observability

- **Emit metrics on everything.** Track request latency, error rates, throughput, cache hit ratio, vendor-call counts, and per-endpoint cost. Expose them in dashboards with SLOs, and treat the four golden signals as non-negotiable.
- **Monitor for data and model drift continuously.** Compare live feature and prediction distributions against training baselines (PSI, KL divergence) and track rolling prediction accuracy. Auto-flag when inputs or performance drift beyond thresholds.
- **Alert on symptoms that matter, with owners.** Page on SLO breaches, stale or missing data feeds, drift threshold breaches, auth/error spikes, and budget overruns. Every alert has a runbook and an on-call owner; no alert without a documented action.
- **Trace requests end to end.** Propagate correlation IDs from API call through model inference to data fetch so any prediction can be reconstructed and debugged.

#### Cost Discipline

- **Cache aggressively at every layer.** Cache vendor responses, computed features, and model outputs keyed by data-snapshot version with explicit TTLs. A request that can be answered from cache must never hit a paid vendor or re-run inference.
- **Respect vendor quotas as hard budgets.** Track per-vendor call counts against contracted limits, throttle before overage fees trigger, and fail gracefully (serve cached/stale-with-warning) rather than blowing past the quota.
- **Watch always-on compute billing.** GPUs, inference endpoints, and managed databases bill by the hour whether idle or not — scale to zero or schedule down off-hours, prefer batch over always-on real-time where latency allows, and alert on any resource running with no traffic.
- **Attribute and review cost continuously.** Tag spend by endpoint, customer, and model so unit economics are visible. Set budget alerts and review the largest cost drivers regularly; an expensive model must justify its price in customer value.

---

## 3. Not To Do (Anti-Patterns)

#### Data
- ❌ Build on IEX Cloud as a data source — it was shut down in August 2024 and no longer exists; any integration is dead on arrival.
- ❌ Depend on unofficial/scraped Yahoo Finance endpoints (e.g. screen-scraping or undocumented query APIs) — they are unstable, unauthorized, rate-limited at will, and break without notice.
- ❌ Re-serve raw licensed vendor data without a redistribution agreement — most market-data licenses forbid redistribution; doing so exposes you to contract termination and legal liability.
- ❌ Ignore corporate actions, splits, and dividends — unadjusted price series produce phantom gaps and gains that silently corrupt features, labels, and backtests.
- ❌ Backtest only on currently-listed (surviving) tickers — survivorship bias inflates returns by excluding delisted/bankrupt names that a real strategy would have held.
- ❌ Skip point-in-time / as-of correctness on fundamentals — using restated or late-arriving data as if it were known on the bar date is a subtle form of lookahead.
- ❌ Cache prices without recording the source, timestamp, and adjustment basis — provenance gaps make bugs unreproducible and license audits impossible.

#### Modeling
- ❌ Feed current-day or future data as features — lookahead bias makes offline metrics look great and live performance collapse.
- ❌ Tune hyperparameters on the test set or reuse it for model selection — it leaks the test distribution and turns the "held-out" score into fiction.
- ❌ Evaluate without walk-forward / time-series cross-validation — random k-fold shuffles future into the training window and overstates accuracy.
- ❌ Assume foundation time-series models (Chronos, TimesFM) beat baselines zero-shot — they frequently do not on noisy financial series; they must earn their place against baselines.
- ❌ Ship without simple baselines (naive last-value, drift, ARIMA, buy-and-hold) — without them you cannot prove the model adds any value.
- ❌ Report only point accuracy / RMSE — for trading, calibration, directional hit-rate, and risk-adjusted returns (after costs) matter more than raw error.
- ❌ Backtest with zero transaction costs, slippage, or fill assumptions — frictionless backtests routinely turn losing strategies into "winners."

#### Legal
- ❌ Market "accurate predictions," "guaranteed returns," or anything implying certainty — it is false advertising and invites regulatory action.
- ❌ Present output as personalized investment advice or recommendations to buy/sell — that can constitute unlicensed advisory activity; keep it informational with clear disclaimers.
- ❌ Omit risk disclaimers and "not financial advice" / "past performance is not indicative of future results" notices — required to set user expectations and limit liability.
- ❌ Use data or model weights with non-commercial / research-only licenses in a paid product — verify every dependency's license permits commercial use and redistribution.

#### Engineering
- ❌ Hardcode secrets, API keys, or DB credentials in source or images — use a secrets manager / environment injection; leaked keys mean data-feed bans and breaches.
- ❌ Let GPU inference endpoints run idle 24/7 — autoscale to zero or batch requests; idle accelerators burn budget for nothing.
- ❌ Serve predictions from an unpinned/unversioned model — without model + data versioning you cannot reproduce, roll back, or audit a bad call.
- ❌ Skip input validation and rate limiting on the API — unbounded or malformed requests invite abuse, runaway cost, and outages.
- ❌ Train and serve with different feature pipelines — train/serve skew silently degrades live predictions versus offline metrics.

#### Product
- ❌ Surface a single point forecast with no uncertainty — always show intervals/confidence so users grasp how unreliable a number is.
- ❌ Imply the product is a substitute for professional financial advice — position it as a research/analytics tool, not a decision-maker.
- ❌ Hide data freshness, coverage, and model limitations from users — silent staleness or gaps erode trust and lead to bad decisions.
- ❌ Optimize the UX to encourage frequent trading on signals — it harms users and amplifies your liability exposure.

---

## 4. Technology Stack & Architecture

#### Core Stack

The service is a Python 3.12 application built on **FastAPI** with full async I/O end to end. FastAPI is served directly by **Uvicorn** with multiple workers in production (`uvicorn app.main:app --workers N`), avoiding the deprecated Gunicorn `uvicorn.workers` path while keeping process-level concurrency plus async request handling within each worker. **Pydantic v2** (Rust-backed `pydantic-core`) is non-negotiable: every request body, response model, and config object is a typed Pydantic model, and we use `pydantic-settings` for environment-driven configuration. All outbound HTTP — market-data pulls, fundamentals, news — goes through **httpx** in async mode with a shared `AsyncClient`, connection pooling, explicit timeouts, and `tenacity`-based retries with backoff. We standardize on Python 3.12 for its faster interpreter and improved typing, and we pin dependencies with **uv** (lockfile-driven, reproducible builds).

#### Data Layer

- **TimescaleDB** (a Postgres extension) is the system of record for all OHLCV and time-series data. Bars live in hypertables partitioned by time (and space-partitioned by symbol where volume warrants), with continuous aggregates materializing 1m -> 5m -> 1h -> 1d rollups, native compression on older chunks, and retention policies. Using a Timescale-flavored Postgres rather than a separate TSDB means time-series and relational data share one engine, one backup story, and one SQL dialect.
- **Postgres** (the same cluster) holds relational metadata: instruments, symbol mappings, corporate actions, users, API keys, model/run registry pointers, and prediction audit records.
- **Redis is split by durability semantics:** `redis-cache` handles response/feature caching and rate-limit counters with LRU eviction and no persistence; `redis-celery` is dedicated to the Celery broker/result backend with AOF persistence and `noeviction`. Pub/sub remains available for fan-out of fresh-bar events, but cache eviction can never delete queued work.

#### Ingestion / Orchestration

We standardize on **Celery + Celery Beat**. Rationale: Celery already has to exist in this stack because synchronous model inference, batch backtests, and heavy feature computation must run off the request thread, and Redis is already deployed as the broker. Adding **Prefect** would mean a second scheduler, a second worker pool, and a second operational surface for the same job: "pull data on a schedule, retry on failure." Beat handles cron-style scheduled pulls (e.g., EOD fundamentals, intraday bar polling, news sweeps); Celery handles retries, rate-limit-aware backoff, idempotent upserts, and concurrency control. If DAG-style data lineage and a richer observability UI become a real requirement later (complex multi-stage backfills, cross-dataset dependencies), Prefect is the migration target — but we do not pay for it on day one.

#### ML / Modeling

A layered model portfolio, from cheap baselines to deep models:

- **Baselines:** `statsmodels` for transparent statistical diagnostics and **Nixtla StatsForecast** for fast, parallel classical models (AutoARIMA, ETS, Theta) at scale.
- **Foundation forecasting:** **Chronos-2**, self-hosted, as the zero/few-shot baseline forecaster — it gives a strong out-of-the-box probabilistic forecast with no per-symbol training, which is ideal for broad coverage.
- **Deep learning:** deferred until the baselines, LightGBM, and Chronos-2 prove insufficient on our own walk-forward evaluation. NeuralForecast / PyTorch Forecasting can be revisited behind a promotion gate; they are not day-one dependencies.
- **Tabular:** **LightGBM** over engineered features (lagged returns, volatility, calendar, cross-sectional, fundamental, and sentiment features) for direction/return regression and ranking.
- **Indicators / features:** owned, unit-tested indicator functions for the small set we actually use (SMA/EMA, returns, volatility, RSI/MACD/Bollinger/ATR as needed). TA-Lib remains optional where a C-backed reference implementation is worth the native dependency.

Every model conforms to a common forecaster interface (`fit` / `predict` / `predict_quantiles`) so they are interchangeable behind the serving layer and comparable in evaluation.

#### Experiment Tracking & Registry

**MLflow** is the single source of truth for experiments and the model registry. Every training run logs params, metrics (per-horizon MAE/MASE/pinball loss, directional accuracy), artifacts, and the resolved feature set; promotion to `Staging`/`Production` in the MLflow Model Registry is what the serving layer reads to decide which model version is live. The local/dev artifact store is a filesystem path backed up offsite; production can move artifacts to a managed S3-compatible bucket (Backblaze B2 / Cloudflare R2 / cloud object storage) if filesystem artifacts stop being enough. We do not run self-hosted MinIO on the VPS. **Feast** is the optional feature store: we adopt it once online/offline feature skew and reuse across models become a real pain, backed by Postgres/Timescale offline and Redis online — not before.

#### Backtesting

Backtesting / evaluation is trust-core IP we own: a small, explicit walk-forward harness for forecast evaluation (pinball loss, CRPS, empirical coverage, directional accuracy, and cost-aware signal checks where relevant), run as Celery jobs and logged to MLflow. Bias-free discipline is enforced as a hard rule, not a guideline: strictly point-in-time data (see Sharadar below for fundamentals), all features computed with as-of timestamps, no lookahead in indicator windows, explicit train/validation/test splits with **walk-forward / purged** cross-validation, realistic transaction costs and slippage, and an embargo between train and test to prevent leakage. Third-party strategy engines are deferred unless the owned harness proves insufficient.

#### Serving

Models are served behind the same **FastAPI** app via a dedicated prediction router that loads versioned artifacts resolved from the MLflow registry (cached in-process, refreshed on promotion). Heavy/batch inference is dispatched to Celery; low-latency single-symbol predictions run inline with Redis-cached results. **BentoML** is the escape hatch if we need independent scaling, adaptive batching, or GPU-backed model servers separate from the API tier. Critically, the API serves **only derived predictions** — forecasts, intervals, signals, scores, and our own computed analytics. We never redistribute raw vendor market data; this is both a licensing requirement and the core product boundary.

#### Data Sources (committed)

- **Polygon / Massive** — primary equities/options price and aggregate bars.
- **FMP (Financial Modeling Prep)** — fundamentals, financial statements, ratios, corporate actions.
- **Finnhub** — news and sentiment.
- **Sharadar** — point-in-time fundamentals specifically for bias-free backtests (as-reported, no restatement leakage).
- **Databento US Equities (Mini)** — the redistribution-safe, normalized upgrade path for prices as volume and licensing needs grow.
- **Alpaca** — paper-trading account for live, forward validation of signals against real fills.
- **Binance / CoinGecko** (crypto) and **OANDA** (FX) — the expansion path beyond US equities.

Each source sits behind a normalizing adapter (a common `MarketDataProvider` / `FundamentalsProvider` protocol) so vendors can be swapped or A/B'd without touching downstream code.

#### API Gateway / Auth

Two-tier auth: **API keys** for programmatic/customer access (hashed at rest, scoped, per-key quotas) and **JWT** for interactive/session use. Rate limiting starts with **slowapi** (Redis-backed, per-key and per-IP) inside the app; **Kong** sits in front as the dedicated API gateway when we need centralized auth, quota tiers, analytics, and routing across multiple services.

#### Observability

**Prometheus** scrapes app, Celery, Postgres/Timescale, and Redis metrics (request latency, queue depth, model inference time, cache hit rate, per-vendor API error rates), visualized in **Grafana** dashboards with alerting. **Sentry** captures exceptions and traces across the API and workers. **Structured logging** (JSON via `structlog`) with request/correlation IDs ties a prediction back through its features, model version, and data pulls.

#### Packaging / CI-CD

Everything is containerized with **Docker** and orchestrated locally and on the VPS via **docker-compose** (services: api, worker, beat, postgres/timescale, redis-cache, redis-celery, mlflow, prometheus, grafana, reverse proxy). **GitHub Actions** runs lint (`ruff`), type-check (`mypy`), tests (`pytest`), builds and pushes images to a registry (GHCR), and deploys. **Kubernetes** is explicitly optional and deferred — adopted only when horizontal scale, multi-node GPU scheduling, or rolling-deploy guarantees outgrow compose.

#### Deployment

Initial target is a **Hostinger VPS** (the team already has a Hostinger account, manageable via the Hostinger API). We provision an **Ubuntu LTS** VPS, install **Docker + docker-compose**, and bring up the full stack from the compose file. A reverse proxy — **Caddy** (automatic TLS) or **Nginx + certbot** — terminates HTTPS and routes to the FastAPI/Kong tier; the Hostinger firewall and an on-host UFW restrict inbound to 80/443/SSH. Secrets come from an `.env` / Docker secrets, snapshots/backups cover Postgres and the MLflow artifact store. This runs the API, workers, databases, and CPU inference (StatsForecast, ARIMA, XGBoost/LightGBM, and quantized Chronos-2) comfortably.

The **cloud path** (AWS / Azure / GCP) is the deliberate upgrade for what a single VPS cannot do well: **managed GPU** for training/serving TFT, N-HiTS, and full-precision Chronos-2 (e.g., SageMaker / Vertex AI / Azure ML), **managed Postgres/Timescale Cloud**, object storage for MLflow artifacts, and **AutoML** offerings for rapid model search. The Docker-first design means the same images move from VPS to cloud unchanged; only the orchestration (compose -> ECS/Kubernetes) and managed-service wiring change.

#### Summary: Layer | Technology | Why

| Layer | Technology | Why |
|---|---|---|
| Language | Python 3.12 | Ecosystem for data/ML; faster interpreter, better typing |
| API framework | FastAPI + Uvicorn | Async, typed, high-throughput; multi-worker ASGI serving |
| Validation/config | Pydantic v2, pydantic-settings | Fast Rust-backed validation; typed env config |
| HTTP client | httpx (async) + tenacity | Async vendor pulls with pooling, timeouts, retries |
| Time-series DB | TimescaleDB (Postgres) | OHLCV hypertables, continuous aggregates, compression, SQL |
| Metadata DB | Postgres | Instruments, users, keys, registry pointers, audit |
| Cache / broker | Split Redis | `redis-cache` for response/feature cache + rate-limit counters; `redis-celery` for Celery broker/results |
| Orchestration | Celery + Beat | Already needed for async jobs; one scheduler, not two |
| Foundation forecasting | Chronos-2 (self-hosted) | Strong zero-shot probabilistic baseline, no per-symbol training |
| Classical forecasting | StatsForecast, statsmodels | Fast, scalable ARIMA/ETS/Theta baselines |
| Deep forecasting | Deferred: NeuralForecast / PyTorch Forecasting | Revisit only after baselines fail a promotion gate |
| Tabular ML | LightGBM | Feature-based direction/return prediction and ranking |
| Indicators | Owned functions (+ optional TA-Lib) | Auditable technical features with golden-value tests |
| Tracking/registry | MLflow (+ optional Feast) | Single source of truth for runs and live model version |
| Backtesting | Owned walk-forward harness | Forecast-first, bias-free evaluation we can audit and explain |
| Serving | FastAPI (BentoML optional) | Serve only derived predictions; Bento for scaled/GPU serving |
| Prices | Polygon/Massive -> Databento Mini | Primary bars; redistribution-safe upgrade path |
| Fundamentals | FMP + Sharadar (PIT) | Live fundamentals; point-in-time for unbiased backtests |
| News/sentiment | Finnhub | News and sentiment signals |
| Validation/expansion | Alpaca (paper); Binance/CoinGecko, OANDA | Forward validation; crypto/FX expansion |
| Auth/gateway | API keys + JWT, slowapi -> Kong | Tiered access; in-app then dedicated gateway rate limiting |
| Observability | Prometheus + Grafana, Sentry, structlog | Metrics, alerting, error tracing, correlated logs |
| Packaging/CI-CD | Docker + docker-compose, GitHub Actions | Reproducible images; automated lint/test/build/deploy |
| Deployment | Hostinger VPS (Ubuntu, Docker, Caddy/Nginx) -> cloud | Cheap CPU start; AWS/Azure/GCP for managed GPU + AutoML |
| Scale (optional) | Kubernetes | Only when horizontal/GPU scheduling outgrows compose |

---

## 5. API & Framework Feature Catalog

This catalog enumerates the full, current (2026) feature set of every API and framework in scope.

### 5.1 Market & Fundamental Data APIs
### Massive (formerly Polygon.io)

- **What it is**: Developer-first market-data platform providing real-time and historical pricing, reference, fundamental, and news data for US equities, options, indices, FX, crypto, and futures via REST, WebSocket, and flat files (Polygon.io rebranded to Massive.com in 2026; same product, endpoints, and pricing).

- **Endpoints / capabilities**:
  - **Aggregates / bars**: OHLCV bars across custom intervals (second/minute/hour/day/week/month), grouped daily bars, previous-close, plus pre/post-market data.
  - **Trades & quotes**: Tick-level historical and real-time trades; NBBO quotes (top-of-book bid/ask).
  - **Snapshots**: Full-market snapshot, single-ticker, gainers/losers, unified/universal snapshot across asset classes.
  - **Reference / metadata**: Tickers list, ticker details, ticker types, exchanges, market status, market holidays, conditions/trade-condition mappings, ticker events.
  - **Fundamentals**: Stock financials (income statement, balance sheet, cash flow) sourced from SEC filings.
  - **News & sentiment**: Market/company news articles with ticker tagging and insights/sentiment; enhanced via Benzinga (analyst ratings, corporate guidance, earnings, structured news).
  - **Corporate actions**: Dividends, stock splits, IPOs; corporate events (earnings dates, etc.) via TMX Wall Street Horizon partnership.
  - **Options / derivatives**: Full US options chains, contracts, trades, quotes, snapshots with bid/ask, last trade, open interest, volume, implied volatility, and all Greeks (delta, gamma, theta, vega).
  - **Technical indicators**: Server-side SMA, EMA, MACD, RSI.
  - **WebSocket / streaming**: Real-time streaming of trades, quotes, aggregates (per-second/per-minute), and options data; multi-subscription on one socket.
  - **Bulk / flat-files**: Daily historical Flat Files (CSV) via web-based File Browser and S3 access; included in all paid plans at no extra charge for rate-limit-free bulk/backtest workflows.

- **Asset classes & coverage**: US Stocks/ETFs (100% market coverage, all tickers), US Options (full chain), Indices, Currencies (Forex, 1,000+ pairs) and Crypto (major exchanges — Coinbase, Kraken, etc.), and Futures (top US contracts ES/GC/CL across CME, CBOT, COMEX, NYMEX). Primarily US-market focused. History depth: 5 yrs (Starter), 10 yrs (Developer), 20+ yrs (Advanced); free tier is end-of-day only.

- **Real-time / delayed / websocket**: Free and lower tiers serve 15-minute delayed (and EOD on free); real-time available on higher tiers — IEX real-time on mid tier, full SIP real-time + WebSocket streaming on Advanced. Futures and other classes offer real-time + historical via REST/WS/flat-files.

- **Auth method**: API key (passed as query param `apiKey` or `Authorization: Bearer` header). No OAuth.

- **Rate limits**: Free tier = 5 API calls/minute. All paid tiers = unlimited API calls (REST). WebSocket and flat-file/S3 access bypass REST request limits.

- **Pricing tiers** (per-asset-class, monthly; Stocks shown):
  - Stocks Starter — **$29/mo**: all US tickers, unlimited calls, 5 yrs history, 15-min delayed.
  - Stocks Developer — **$79/mo**: unlimited calls, 10 yrs history, 15-min delayed (real-time IEX feed at this level).
  - Stocks Advanced — **$199/mo**: unlimited calls, 20+ yrs history, full real-time SIP + WebSocket.
  - Options, Indices, Currencies (FX/Crypto), and Futures are priced as separate per-asset-class subscriptions on analogous Starter/Developer/Advanced tiers; Enterprise/business plans available for custom licensing.

- **Free tier**: Yes — free API key with 5 calls/min, end-of-day data, no credit card required (good for prototyping; not for real-time/intraday production).

- **SDKs / official client libraries**: Official clients for **Python** (`polygon-api-client`), **JavaScript/Node**, **Go**, and **JVM/Kotlin (Java)**; official MCP server also available. Popular third-party Python wrapper (`polygon`) exists. Official libraries are MIT-licensed.

- **Licensing / redistribution notes**: Subscription grants internal/personal use of the data; standard plans are non-sublicensable and non-transferable (no redistribution of raw data to third parties). Public redistribution, resale, or display to end-users requires an Enterprise/business agreement. ML-training rights are not covered by standard plans — using market data to train/redistribute models for a product warrants explicit confirmation via an Enterprise license. (Client SDK code is MIT; the data license is separate and more restrictive.)

- **Role in OUR stack**: Primary ingestion source for a stock-analysis + prediction API. Use REST aggregates/bars + Flat Files (S3) for bulk historical OHLCV to train/backtest models; reference + corporate-actions endpoints for split/dividend adjustment and clean symbol mapping; fundamentals + Benzinga news/sentiment as model features; technical-indicator endpoints to offload SMA/EMA/MACD/RSI computation; WebSocket streaming (Advanced tier) for live inference and real-time quote/trade serving. Start on Stocks Starter/Developer for prototyping, move to Advanced (+ Options/Indices/Futures add-ons as needed) for production real-time; secure an Enterprise/redistribution + ML-training license before surfacing raw Massive data or model outputs derived from it to external customers.

Sources:
- [Pricing | Massive](https://massive.com/pricing)
- [Stocks REST API Overview | Massive](https://massive.com/docs/rest/stocks/overview)
- [Polygon.io is Now Massive | Massive](https://massive.com/blog/polygon-is-now-massive)
- [Changelog | Massive](https://massive.com/changelog)
- [Futures Data API | Massive](https://polygon.io/futures)
- [RESTful API request limit | Massive KB](https://polygon.io/knowledge-base/article/what-is-the-request-limit-for-polygons-restful-apis)
- [Official Python client (GitHub)](https://github.com/polygon-io/client-python)
- [Official JVM client (GitHub)](https://github.com/polygon-io/client-jvm)
- [Market Data APIs Compared 2026 — AI Fin Hub](https://aifinhub.io/articles/market-data-apis-compared-2026/)
### Databento US Equities (incl. US Equities Mini)

- **What it is:** Institutional-grade, normalized market data platform delivering real-time and historical US equities (and futures/options) data via a single unified API, sourced from colocated direct/proprietary feeds with nanosecond timestamps.

- **Endpoints / capabilities:**
  - **Historical API** (REST/HTTP, organized into four endpoint groups): `metadata` (dataset listings, schemas, fields, costing via `metadata.get_cost`, conditions, ranges), `timeseries` (the core data-pull endpoint `timeseries.get_range` / streaming), `symbology` (`symbology.resolve` — map between raw symbols, instrument IDs, continuous/parent symbols), and `batch` (submit + download large jobs).
  - **Live API:** low-latency streaming subscription gateway (binary DBN encoding over TCP), same schemas/interfaces as historical for replay-to-live code parity; intraday replay + real-time.
  - **Reference API** (HTTP + Python): `corporate_actions`, `adjustment_factors`, `security_master` — metadata/fundamentals-adjacent reference data, not price ticks.
  - **Schemas (the "feature" surface, shared across all asset classes):** `mbo` (L3, market-by-order), `mbp-10` (L2, 10-level depth), `mbp-1`/`bbo-1s`/`bbo-1m` (L1 top-of-book), `tbbo` (BBO sampled on each trade), `trades` (every print), `ohlcv-1s/1m/1h/1d` (aggregate bars), `definition` (instrument/security definitions), `statistics` (venue official stats — open/close/settle), `status` (trading status), `imbalance` (auction imbalance).
  - **Bulk / flat-files:** batch download to compressed DBN/CSV/JSON flat files; pay-as-you-go GB-metered historical extraction (~$0.40/GB for US Equities).
  - **No** native technical-indicators or news/sentiment endpoints (you compute indicators yourself from OHLCV/ticks).

- **Asset classes & coverage:** US equities, futures, and options (unified API); FX/crypto not core. Equities spans **60+ US trading venues / 40+ NMS exchanges & ATSs**, **20,000+ symbols**. Datasets: **Databento US Equities** (full L3/L2/L1 order book, history from **2018**), **US Equities Mini** (composite top-of-book BBO + trades, history from **March 28, 2023**), **US Equities Summary** (EOD consolidated OHLCV/volume). Underlying venue feeds include Nasdaq TotalView-ITCH, etc. ~19 PB total historical coverage.

- **Real-time / delayed / websocket:** Real-time (sub-microsecond-accurate, NY4-colocated prop feeds), full historical replay, and delayed/EOD (Summary dataset = 100% delayed consolidated). Streaming is via Databento's binary DBN protocol over a persistent TCP session (client libraries abstract it), not a browser websocket.

- **Auth method:** API key (Bearer/HTTP Basic with the key as username); keys managed in the portal. No OAuth. Session/usage limits are enforced per user, not per key.

- **Rate limits:** Live API simultaneous **sessions limited to 10 or 50 connected applications per user** depending on plan (changed Feb 16, 2025; previously 100). Limit is per-user (extra API keys don't raise it; add team users for more). Historical is GB/usage-metered rather than hard request-rate-capped, with guidance to avoid excessive bursts.

- **Pricing tiers (current):** **Standard plan $199/month** — unlimited access to 7 years of OHLCV history, 12 months of L0/L1 across 12 schemas, 1 month of L2 (MBP-10) and L3 (MBO/Imbalance). CME live (GLBX MDP 3.0) **Standard plan $179/month** (usage-based CME *live* pricing discontinued Apr 16, 2025). **Historical remains pay-as-you-go / metered** (e.g., ~$0.40/GB US Equities) and is not being removed. US Equities Mini avoids per-exchange license fees (its key cost advantage).

- **Free tier:** New users receive **$125 in free API credits** automatically on signup; non-professional, non-redistributing users can sign an attestation for instant real-time access (Databento acts as vendor of record).

- **SDKs / official client libraries:** **Python**, **Rust** (tokio-based), and **C++** (C++17, CMake 3.24+) for both historical and live; plus raw HTTP/REST. Python integrates directly with pandas/Polars. Third-party integrations exist (NautilusTrader, Lumibot).

- **Licensing / redistribution notes:** Non-professional + non-redistributing → instant attestation, no exchange ILA needed. Professional use or any redistribution → Databento brokers the exchange license/ILA (some historical redistribution requires a license). US Equities Mini is **derived data** with no exchange license fees and instant approval, but is contractually restricted: it **cannot be reverse-engineered to the original feeds** and is **not a substitute for the original prop feeds**. No explicit published ML/AI-training grant — derived/internal use is the safe default; confirm AI-training/model-distribution rights directly with Databento before training on or redistributing derived outputs.

- **Role in OUR stack:** Primary high-fidelity ingestion layer for a stock analysis + prediction API. Use **US Equities Mini** (BBO + trades) as the cost-effective real-time/intraday top-of-book feed and **US Equities Summary** for EOD bars; pull **OHLCV** and **tick/MBP-10** history via the Historical batch/timeseries API to build training datasets and backtests. Use the **Reference API** (`corporate_actions` + `adjustment_factors` + `security_master`) to back-adjust prices, normalize symbology, and keep a clean instrument universe. Code once against the unified schemas so the same pipeline serves backtest (historical replay) and live inference; engineer technical indicators/features in-house since Databento ships raw normalized data, not derived signals. Mind redistribution limits — if the prediction API exposes raw quotes/prices downstream, secure the appropriate ILA/redistribution license first.
### Financial Modeling Prep (FMP) API

- **What it is**: A REST + WebSocket financial data API providing real-time/historical market prices and deep company fundamentals across global asset classes, aimed at developers, fintechs, and quant/ML workflows.

- **Endpoints / capabilities** (100+ endpoints, JSON & CSV):
  - **Prices / charts / aggregates**: EOD historical daily prices, intraday charts (1min/5min/15min/30min/1hour/4hour), adjusted & split/dividend-adjusted series, historical market cap.
  - **Quotes / trades**: real-time full quotes, batch quotes, price-change snapshots, pre/post-market, aftermarket trade/quote.
  - **Reference / metadata**: company profile, symbol lists, tradable/searchable symbol search (name/ticker/CIK/CUSIP/ISIN), exchange & sector lists, market hours, peers, employee count, executive compensation.
  - **Fundamentals**: income statement, balance sheet, cash flow (annual/quarter, as-reported & standardized), financial ratios, key metrics, enterprise value, financial growth, DCF / advanced DCF & levered DCF, financial score, owner earnings.
  - **News / sentiment**: general/stock/crypto/forex/press-release news, articles, social sentiment, historical/trending sentiment.
  - **Corporate actions / calendars**: dividends, stock splits, earnings calendar & historical earnings, IPO calendar, mergers & acquisitions.
  - **Ownership / holdings**: institutional 13F holdings, ETF & mutual fund holdings/holders, insider trading, Senate/House (congressional) trading, fund/share-float data.
  - **Analyst data**: analyst estimates, price targets, upgrades/downgrades, ratings, grades-consensus.
  - **Earnings transcripts**: full earnings call transcripts (10+ years).
  - **Technical indicators**: SMA, EMA, WMA, DEMA, TEMA, RSI, ADX, Williams %R, Standard Deviation.
  - **Economics / macro**: treasury rates, economic indicators (GDP, CPI, unemployment, etc.), market risk premium, economic release calendar, commodities.
  - **Options/derivatives**: limited — FMP is primarily equities/fundamentals; no full options chains/greeks (notable gap vs. options-focused vendors); short-interest also not provided.
  - **WebSocket / streaming**: real-time streaming for stocks, crypto, and forex.
  - **Bulk / flat-files**: Bulk Download endpoints (CSV) for full-universe profiles, prices, ratios, statements, peers — built for scalable dataset/model ingestion.

- **Asset classes & coverage**: US stocks & ETFs, mutual funds, forex, crypto, commodities, indices, and macroeconomic data. Geography expands by tier — US-only (Starter) → +UK/Canada (Premium) → global/70+ exchanges (Ultimate). History depth: up to 5 years (Starter), up to 30+ years (Premium/Ultimate) for prices & fundamentals; 10+ years of transcripts/estimates. Limited/no options chains and no short-interest data.

- **Real-time / delayed / websocket**: Free/Basic tier is EOD-only (delayed/end-of-day). Paid tiers provide real-time US data; WebSocket real-time streaming (stocks/crypto/FX) on paid tiers. Intraday charts gated to Premium+; 1-minute intraday + bulk to Ultimate.

- **Auth method**: API key only (no OAuth). Passed as a query parameter `apikey=YOUR_API_KEY` on every request.

- **Rate limits** (current tiers): Free ~250 calls/day; Starter ~300 calls/min; Premium ~750 calls/min; Ultimate ~3,000 calls/min. Trailing-30-day bandwidth caps also apply: Free 500MB, Starter 20GB, Premium 50GB, Ultimate 150GB, Enterprise 1TB+.

- **Pricing tiers** (current monthly USD): Basic/Free $0; Starter ~$22/mo; Premium ~$59/mo; Ultimate ~$149/mo; Enterprise custom (annual contracts, separately negotiated; broader datasets/redistribution). Note: a flat ~$19/mo real-time tier (REST + WebSocket) is also promoted; exact figures shift with periodic repricing/promos.

- **Free tier**: Yes — free API key, ~250 market-data calls/day, 500MB/30-day bandwidth, end-of-day data only, restricted to personal/non-commercial use; good as a testing/exploration sandbox.

- **SDKs / official client libraries**: REST + WebSocket plus Excel Add-in and Google Sheets add-on; official Python SDK referenced on the FMP resources page. Ecosystem is largely community-maintained — popular Python wrappers include `fmpsdk` (daxm/fmpsdk), `fmp-py` (TexasCoding), thinh-vu/FinancialModelingPrep, `fmp_python`, and an R client (`r-fmpapi` / tidy-finance). Also distributed via AWS Marketplace.

- **Licensing / redistribution notes**: Standard license is limited, revocable, non-transferable, non-sublicensable, non-exclusive — internal/personal use only. Public display, redistribution, resale, sublicensing, or building third-party-facing tools requires a separate **Data Display and Licensing Agreement** (Enterprise). No explicit ML-training grant in standard terms — training/derivative-dataset rights and any data redistribution must be negotiated under Enterprise/commercial licensing. Plan accordingly: a customer-facing prediction product almost certainly needs the commercial/redistribution agreement.

- **Role in OUR stack**: Primary fundamentals + historical-price backbone for a stock analysis & prediction API. Use Bulk/flat-file endpoints to ingest the full universe (profiles, daily prices, statements, ratios, estimates) into our feature store for model training; use intraday + WebSocket for live features and near-real-time scoring; pull earnings transcripts, analyst estimates, insider/13F/congressional ownership, and news sentiment as alpha-signal features; use DCF/ratios/key-metrics endpoints to power fundamental valuation outputs. Caveat: no options chains/greeks and no short-interest (supplement from an options-focused vendor if needed), and we'd need an Enterprise Data Display/redistribution + ML-training license before surfacing FMP-derived data or model outputs to external customers.

Sources: [FMP Pricing](https://site.financialmodelingprep.com/developer/docs/pricing), [FMP Docs](https://site.financialmodelingprep.com/developer/docs), [Find My Moat FMP Review 2026](https://www.findmymoat.com/tools/financial-modeling-prep-fmp), [FMP Terms of Service](https://site.financialmodelingprep.com/terms-of-service), [FMP Bulk Endpoints](https://site.financialmodelingprep.com/developer/docs/bulk-endpoints), [fmpsdk](https://github.com/daxm/fmpsdk)
### Tiingo

- **What it is**: A low-cost financial markets data API providing end-of-day and intraday prices, fundamentals, curated news, crypto, and FX, with proprietary error-corrected EOD data and 30+ years of history.

- **Endpoints / capabilities**:
  - **EOD (Daily) prices** — `/tiingo/daily/<ticker>` metadata, `/prices` latest, and historical OHLCV with split/dividend-adjusted columns, validated by Tiingo's proprietary EOD Price Engine.
  - **IEX intraday / real-time** — `/iex` and `/iex/<ticker>` for top-of-book/last; `/iex/<ticker>/prices` for historical intraday with `resampleFreq` (1min, 5min, etc.); supports extended/pre-post-market; IEX cross-connect option for nanosecond tick data.
  - **Fundamentals** — definitions endpoint, daily metrics (valuation ratios, market cap, P/E, dividend yield), and quarterly/annual financial statements (income statement, balance sheet, cash flow); 80+ indicators.
  - **News / sentiment** — curated articles from top financial sources and blogs, algorithmically tagged with topic tags and relevant tickers; filterable by ticker, source, tag, date.
  - **Crypto** — `/tiingo/crypto/top` top-of-book, `/tiingo/crypto/prices` real-time and historical (resampled), aggregating multiple exchanges.
  - **Forex (FX)** — `/tiingo/fx/<ticker>/top` and `/prices` historical intraday with resampling; 140+ pairs.
  - **Corporate actions** — dividends (distributions) and splits, both historical and announced.
  - **Reference / metadata** — ticker metadata, supported-ticker lists/files, exchange and asset-type info.
  - **WebSocket / streaming** — separate streaming endpoints for IEX equities (`wss://api.tiingo.com/iex`), Forex (`wss://api.tiingo.com/fx`), and Crypto (`wss://api.tiingo.com/crypto`).
  - No native options, futures, or built-in technical-indicator endpoints.

- **Asset classes & coverage**: U.S. stocks, ETFs, mutual funds, ADRs plus select international equities; crypto (multi-exchange aggregated); FX (140+ pairs). Fundamentals span 20+ years across ~5,500+ equities. Price history is 30+ years for EOD. No options/futures.

- **Real-time / delayed / websocket**: IEX equities real-time via REST + WebSocket (full TOPS feed requires a signed IEX market-data agreement as of Feb 2025; otherwise a free derived/reference price is provided). Crypto and FX real-time via REST + WebSocket. EOD is end-of-day batch.

- **Auth method**: API key (token), passed via `Authorization` header or `token` query parameter. No OAuth.

- **Rate limits** (current tiers):
  - **Free**: 50 requests/hour, 1,000/day, ~500 unique symbols/month, 1 GB/month bandwidth.
  - **Power**: ~10,000 requests/hour, 100,000/day, 40 GB/month, unlimited symbols.
  - **Commercial Power**: ~20,000 requests/hour, 150,000/day, 100 GB/month. Limits are approximate and can be raised by emailing sales for reasonable requests.

- **Pricing tiers** (current $): **Starter/Free** — $0. **Power (individual/non-commercial)** — $30/month or $300/year. **Commercial / Business** — ~$50/month (commercial-use license with higher limits). Fundamentals access is a paid add-on with separate commercial licensing (contact sales). Enterprise/custom available.

- **Free tier**: Yes — permanent free Starter plan with 50 req/hr, 1,000 req/day, 500 symbols/month, 1 GB bandwidth; includes EOD, IEX, news, fundamentals (definitions/daily/dividend-yield), crypto, and FX. Dividends (distributions) and splits endpoints return 403 on free tier; full commercial-grade fundamentals statements require a paid plan.

- **SDKs / official client libraries**: Python (`tiingo` / tiingo-python, returns JSON or pandas DataFrames) is the primary maintained client; R (`riingo`); plus third-party SDKs and integrations (AmiBroker, Excel, community wrappers, and MCP servers). REST returns JSON or CSV.

- **Licensing / redistribution notes**: Free/Power plans are for internal, non-commercial/individual use; commercial use requires the Commercial license. It is explicitly **not a blanket redistribution license** — you cannot resell or broadly redistribute raw data without separate terms. Fundamentals data carries its own commercial-licensing terms (third-party-sourced). ML-training/model use is not an advertised standard right — confirm with sales for redistribution or training-data usage.

- **Role in OUR stack**: Use Tiingo as the cost-efficient primary feed for a stock analysis + prediction API — pull 30+ years of error-checked adjusted EOD OHLCV for model training/backtesting, IEX intraday + WebSocket for live/near-real-time inputs, fundamentals (statements + daily ratios) and corporate actions as model features, and the tagged news feed for sentiment signals. Auth via a single API key, batch nightly EOD ingestion within daily limits, and stream IEX/crypto/FX for real-time scoring. Validate the Commercial license tier before exposing derived predictions externally, and budget a separate provider for options/futures since Tiingo lacks those.

Sources: [tiingo.com/about/pricing](https://www.tiingo.com/about/pricing), [findmymoat.com/tools/tiingo](https://www.findmymoat.com/tools/tiingo), [tiingo.com/documentation/iex](https://www.tiingo.com/documentation/iex), [tiingo.com/documentation/end-of-day](https://www.tiingo.com/documentation/end-of-day), [tiingo.com/documentation/crypto](https://www.tiingo.com/documentation/crypto), [tiingo.com/documentation/forex](https://www.tiingo.com/documentation/forex), [github.com/major7apps/tiingo-mcp](https://github.com/major7apps/tiingo-mcp/blob/main/CLAUDE.md), [tiingo-python.readthedocs.io](https://tiingo-python.readthedocs.io/)

I now have a solid, verified picture. Here is the final markdown block.

### Finnhub Stock API

- **What it is:** Institutional-grade financial data REST + WebSocket API covering real-time prices, global fundamentals, estimates, news/sentiment, and alternative data for stocks, FX, and crypto.

- **Endpoints / capabilities:**
  - **Quotes / trades:** real-time `quote`, `last_bid_ask`, historical NBBO/BBO (`stock_nbbo`), tick data (`stock_tick`), WebSocket trade & quote streaming.
  - **Prices / aggregates:** OHLCV candles (`stock_candles`, `forex_candles`, `crypto_candles`).
  - **Reference / metadata:** `stock_symbols`, `symbol_lookup`, `company_profile`/`company_profile2`, `company_peers`, `company_executive`, `country`, `market_status`, `market_holiday`, `symbol_change`/`isin_change`, `bank_branch`.
  - **Fundamentals / financials:** `company_basic_financials` (metrics), `financials`, `financials_reported` (as-reported), `company_earnings`, `price_metrics`, `sector_metric`, `company_esg_score`, `company_earnings_quality_score`, `revenue_breakdown`.
  - **Estimates / research:** `price_target`, `recommendation_trends`, `upgrade_downgrade`, EPS/revenue/EBITDA/EBIT estimates.
  - **News / sentiment:** `general_news`, `company_news`, `press_releases`, `news_sentiment`, `stock_social_sentiment`, SEC sentiment/similarity analysis.
  - **Corporate actions:** `stock_dividends`/`stock_basic_dividends`, `stock_splits`, earnings call `transcripts`.
  - **Calendars:** earnings, IPO, economic, FDA.
  - **Ownership / institutional:** `ownership`, `fund_ownership`, institutional profile/portfolio/ownership (13F), `congressional_trading`.
  - **Alternative data:** insider transactions & sentiment, USPTO patents, visa applications, supply chain, lobbying, USA government spending, investment themes.
  - **Technical indicators:** `technical_indicator`, `pattern_recognition`, `support_resistance`, `aggregate_indicator`.
  - **ETFs / mutual funds / indices / bonds:** ETF & mutual-fund profile/holdings/sector/country exposure (+EET/PAI), index constituents (current & historical), bond profile/price/yield-curve/tick.
  - **Filings & economic:** SEC/international `filings`, `economic_data`/`economic_code`, `forex_rates`.
  - **Streaming:** WebSocket for live trades/quotes (stock, FX, crypto).

- **Asset classes & coverage:** US + global stocks/ETFs/mutual funds, indices, bonds, forex, crypto (no listed exchange-traded options/futures chains). 60+ global exchanges; fundamentals for ~global equities; dividends history up to ~30 years; deep candle/tick history on paid tiers.

- **Real-time / delayed / websocket:** Real-time US quotes on free tier; real-time international and full tick/NBBO require paid market-data plans; WebSocket streaming (≤50 symbols free, unlimited paid).

- **Auth method:** API key (token query param / `X-Finnhub-Token` header); no OAuth.

- **Rate limits:** Free = 60 calls/min (with ~30 calls/sec burst cap); paid plans remove the per-minute cap (subject to ~30 calls/sec) with higher throughput by tier.

- **Pricing tiers:** Modular — Market Data Basic from ~$49.99/mo; Fundamentals from ~$50/mo; Estimates ~$75/mo; Economic data ~$50/mo; real-time international feeds priced per market (~$50/mo each); enterprise All-In-One ~$3,500/mo (transcripts add-on separate). Premium consumer bundles span roughly $11.99–$99.99/mo.

- **Free tier:** Yes — 60 calls/min, real-time US quotes, company news, basic fundamentals, SEC filings, WebSocket (≤50 symbols), 1+ year of US candles; international/full-history/alt-data gated to paid.

- **SDKs / official client libraries:** Python, JavaScript/Node.js, Go, Ruby, R, Kotlin, PHP (plus community Rust/Elixir); OpenAPI spec available.

- **Licensing / redistribution notes:** API key is for internal/application use; redistribution of raw data and large-scale display require explicit commercial/enterprise licensing; no general ML-training redistribution rights without an enterprise agreement — confirm scope with Finnhub sales for any downstream resale or model-training use.

- **Role in OUR stack:** Primary ingestion layer for a stock analysis + prediction API — pull real-time/historical candles and quotes (REST + WebSocket) as the price feature source, fundamentals/financials/estimates and recommendation trends as model features, and news/social/insider sentiment as alternative-data signals; use earnings/IPO/economic calendars for event windows and `symbol_lookup`/profiles for reference normalization. Start on the free tier for prototyping, then add Market Data Basic + Fundamentals (+ real-time market add-ons) for production coverage.

I now have enough detail to compile a dense, accurate profile.

### Twelve Data API
- **What it is:** A unified financial-market-data platform delivering multi-asset real-time and historical prices, 100+ technical indicators, fundamentals, and WebSocket streaming through a single REST + WebSocket API (base: `https://api.twelvedata.com`, `wss://ws.twelvedata.com`; JSON/CSV output).
- **Endpoints / capabilities:**
  - **Prices / aggregates:** `time_series` (OHLCV, intervals 1min→1month), `eod` (end-of-day), `time_series_cross` (cross rates).
  - **Quotes / latest:** `quote`, `price` (latest real-time price), `market_movers` (gainers/losers/actives).
  - **Reference / metadata:** asset catalogs (`stocks`, `forex_pairs`, `cryptocurrencies`, `etfs`, `funds`, `bonds`, `commodities`, `indices`), `symbol_search`, `cross_listings`, `earliest_timestamp`, `exchanges`, `exchange_schedule`/`market_state`, `cryptocurrency_exchanges`, `countries`, `instrument_type`.
  - **Fundamentals:** company `profile`, `statistics`, financial statements (`income_statement`, `balance_sheet`, `cash_flow`), `market_cap`, `key_executives`, `logo`.
  - **News / sentiment:** limited — primary strength is price/fundamentals, not a dedicated news/sentiment feed.
  - **Corporate actions:** `dividends`, `splits`, `earnings`, `earnings_calendar`, `dividends_calendar`, `splits_calendar`, IPO/rights data.
  - **Analysis & estimates:** analyst `recommendations`, `price_target`, EPS/revenue `estimates`, EPS trend/revisions.
  - **ETF / mutual fund:** composition/holdings, performance, risk metrics, NAV.
  - **Options / derivatives:** options chains/expirations available as a data add-on (limited vs. equity coverage); fixed income / bonds available from higher tiers.
  - **Technical indicators:** 100+ indicators (SMA/EMA, RSI, MACD, Bollinger Bands, %B, ADX, ATR, Stochastics, Ichimoku, etc.) callable on the time-series endpoint.
  - **Regulatory data:** EDGAR filings, insider transactions, institutional holders.
  - **WebSocket / streaming:** real-time tick/price streaming with same symbol structure as REST.
  - **Bulk / batch:** batch (multi-symbol) requests, "complex data" / advanced multi-endpoint requests; spreadsheet add-ins (Excel/Google Sheets).
- **Asset classes & coverage:** Equities, ETFs, mutual funds (200k+), forex (140 currencies), crypto, commodities, bonds/fixed income, indices. ~1,000,000 instruments; all US exchanges + 90+ international exchanges across 50–70+ countries, 180+ crypto exchanges. History depth up to ~30 years EOD; intraday granularity from 1 minute.
- **Real-time / delayed / websocket:** Real-time US equities/ETFs/forex/crypto on all tiers; real-time EU and delayed AU/global markets unlock at Pro+. WebSocket streaming requires Pro (individual) / Venture (business) or above; lower tiers get only trial WS credits.
- **Auth method:** API key — passed as `?apikey=` query param or `Authorization: apikey <key>` header (recommended). No OAuth.
- **Rate limits (current tiers):** Free Basic 8 credits/min, 800/day. Individual: Grow ~377 credits/min (no daily cap), Pro ~1,597/min, Ultra ~10,946/min. Business: Venture ~2,584/min, Enterprise 10,946+/min. Limits are credit-based (one call can cost multiple credits).
- **Pricing tiers (current USD):**
  - Individual: Basic free; Grow $79/mo ($66 annual); Pro $229/mo ($191 annual); Ultra $999/mo ($832 annual).
  - Business: Basic free; Venture $414–$499/mo; Enterprise $916–$1,099/mo; Enterprise+ custom. ~17% off annual; student discount available.
- **Free tier:** Basic — $0, 8 credits/min & 800 calls/day, 3 markets, real-time US equities/ETFs/forex/crypto, reference data, technical indicators, spreadsheet add-ins, plus 8 trial WebSocket credits. Internal/non-display use only.
- **SDKs / official client libraries:** Python (`twelvedata-python`), Node.js, Go, Java, R, plus a CLI and an official MCP (Model Context Protocol) server for LLM access. Community libraries also exist; available on RapidAPI.
- **Licensing / redistribution notes:** Default grant is a limited, non-exclusive, non-transferable, non-sublicensable license for **internal use** only. Display to external/authorized users requires a display-rights tier (Venture+); redistribution/external distribution requires a separate redistribution add-on or written agreement and applies to both real-time and delayed feeds. Derived data is allowed only if it cannot be reverse-engineered back to the source data. No explicit ML/AI-training permission is published — model-training rights are not clearly granted and should be confirmed contractually (likely treated under derived-data/redistribution terms). Enterprise+ adds white-labeling, custom exchange licenses, and SOC 2 / ISO 27001 compliance.
- **Role in OUR stack:** Strong single-vendor backbone for a stock analysis + prediction API — pull `time_series` OHLCV (intraday→30yr) for model training/backtests, fundamentals + estimates + corporate actions as predictive features, server-side technical indicators to avoid recomputation, and WebSocket streaming for live inference/alerts. Credit-based limits and the no-daily-cap Grow/Pro tiers suit batch feature pipelines. For a customer-facing product, budget for Venture+ (external display) or a redistribution add-on, and get written clarification on ML-training rights before training on the feed.

Sources:
- [Individual Pricing](https://twelvedata.com/pricing)
- [Business Pricing](https://twelvedata.com/pricing-business)
- [API Documentation](https://twelvedata.com/docs)
- [Introduction to Twelve Data](https://support.twelvedata.com/en/articles/5609168-introduction-to-twelve-data)
- [SDKs and libraries](https://support.twelvedata.com/en/articles/5304475-sdks-and-libraries)
- [Twelve Data Python Client](https://github.com/twelvedata/twelvedata-python)
- [Twelve Data MCP Server](https://github.com/twelvedata/mcp)
- [Terms of use](https://twelvedata.com/terms)
- [Commercial and personal usage](https://support.twelvedata.com/en/articles/5332349-commercial-and-personal-usage)

I have enough verified data. Exact dollar pricing is gated behind Nasdaq login, but the long-standing published Sharadar rates are well-established in the public record. I'll present them with the appropriate caveat. Compiling the final markdown block.

### Sharadar Core US Equities Bundle (Nasdaq Data Link)
- **What it is:** Point-in-time, survivorship-bias-free US equity fundamentals and end-of-day prices (plus insider, institutional, and corporate-event data) delivered as queryable "datatables" via Nasdaq Data Link; Sharadar was acquired by Nasdaq in 2018.
- **Endpoints / capabilities:**
  - **Fundamentals (`SHARADAR/SF1`):** ~150 standardized indicators (income statement, balance sheet, cash flow, ratios, valuation) with point-in-time dimensions — `ARQ`/`MRQ` (as-reported vs most-recent quarterly), `ARY`/`MRY` (annual), `ART`/`MRT` (trailing-twelve-month); selectable to include or exclude restatements; time-indexed to both filing date (`datekey`) and fiscal period (`calendardate`).
  - **Daily metrics (`SHARADAR/DAILY`):** daily-updated market cap, EV, P/E, P/B, P/S, EV/EBITDA, dividend yield.
  - **Equity prices (`SHARADAR/SEP`):** end-of-day OHLCV, dividend-and-split adjusted close, dividends.
  - **Fund prices (`SHARADAR/SFP`):** EOD prices for ETFs, ETNs, CEFs (separate ETF product/bundle, `SFB`).
  - **Reference / metadata (`SHARADAR/TICKERS`):** ticker, name, exchange, sector/industry (SIC), category, first/last trade dates, SEC CIK, permaticker for delisted-name continuity; plus `SHARADAR/INDICATORS` (field dictionary).
  - **Corporate actions (`SHARADAR/ACTIONS`):** splits, dividends, mergers, listings/delistings, ticker changes.
  - **Index constituents (`SHARADAR/SP500`):** historical S&P 500 add/remove events (history to 1957).
  - **Events (`SHARADAR/EVENTS`):** SEC Form 8-K material-event codes (history from 1993).
  - **Insiders (`SHARADAR/SF2`):** Form 3/4/5 insider holdings & transactions.
  - **Institutional (`SHARADAR/SF3`):** Form 13F holdings, summarized by ticker and by investor, categorized into common, funds, calls, puts, warrants, preferred, debt.
  - **No quotes/trades tick data, no news/sentiment, no options chains/Greeks, no technical-indicator endpoint, no streaming/websocket.**
  - **Bulk / flat-files:** entire-table export via `qopts.export=true` (async zipped CSV); column selection (`qopts.columns`), server-side row filtering, cursor pagination.
- **Asset classes & coverage:** US-listed equities only (plus ETFs/ETNs/CEFs via the fund product). SF1 covers 150 indicators across 14,000+ companies (>5,000 active, >9,000 delisted), history from 1997, including foreign issuers (ADRs, Canadian) trading on US markets. SEP: 16,000+ active/delisted tickers (a superset incl. preferred/warrants), history from 1998. SF2 insiders: 15,000+ issuers / 200,000+ insiders from 2005. SF3 institutional: 20,000+ issuers / ~6,000 investors from 2013. No FX, crypto, futures, or non-US geographies.
- **Real-time / delayed / websocket:** End-of-day only — no intraday/real-time and no websocket. Tables update twice each business day, ~17:30 and ~23:30 ET; reporting lag < 1 day.
- **Auth method:** API key (token appended as `api_key=` query param or set in the client library); no OAuth.
- **Rate limits:** Anonymous: ~20 calls/10 min, 50/day. Free authenticated (no premium sub): 300 calls/10 sec, 2,000/10 min, 50,000/day, concurrency of 1. Premium subscriber: 5,000 calls/10 min, 720,000/day; bulk-export downloads capped at 60/hour. Standard API responses are truncated at 10,000 rows (use pagination or bulk export).
- **Pricing tiers:** Per-dataset, a-la-carte subscriptions with non-professional (personal) vs professional/commercial license tiers; the bundle (`SFA`) is discounted vs buying datasets individually. Exact figures are gated behind a Nasdaq Data Link login and are quote/login-based as of 2026; the long-standing published rates have been roughly: SF1 Fundamentals and SEP Prices around the low tens of USD/month each for non-professional and on the order of ~$150/month each for professional, with the full Core US Equities Bundle in the few-hundred-USD/month range for commercial use. Treat these as indicative — confirm current numbers on the SF1/SEP/SFA pricing pages after login.
- **Free tier:** Free Nasdaq Data Link account grants an API key and a free preview/sample (limited tickers and/or table history) for SF1/SEP and a free data sample on request; full coverage requires a paid subscription.
- **SDKs / official client libraries:** Official Nasdaq Data Link clients — Python (`nasdaq-data-link`, formerly `quandl`), R, Excel add-in, plus the documented REST/datatables HTTP API (CSV/JSON/XML). Widely integrated by third parties (e.g., QuantRocket, QuantConnect).
- **Licensing / redistribution notes:** Subscriptions are internal-use by default. An internal/enterprise license permits unlimited internal use including non-display use and creation of derived data (which generally covers using the data to train/backtest ML models internally). Sharing or redistributing the data — or derived data — inside a larger org or externally requires a separate institutional/distribution license. Any public display of the data or derivations must carry "Data from Sharadar" attribution (hyperlinked to sharadar.com online). Misuse can result in cancellation without refund.
- **Role in OUR stack:** Ideal point-in-time backbone for a stock-analysis + prediction API: pull SF1 (ARQ/ART, restatement-aware) for survivorship-bias-free fundamental features and SEP for EOD price/return labels and adjusted series, joined via `permaticker`/`datekey` to avoid look-ahead bias when constructing training datasets; layer DAILY valuation ratios, ACTIONS for clean split/dividend adjustment, SP500 for universe membership, and SF2/SF3 for insider/institutional signal features. Bulk-export the tables nightly into our own store (the twice-daily EOD cadence fits a batch ingestion + scheduled re-train/scoring pipeline). Caveat: EOD-only and US-only — it cannot serve intraday/real-time prediction or non-US assets, so pair it with a separate real-time/intraday and international price feed for live serving.

I have enough verified detail. Here is the dense markdown block.

### Alpaca Trading API & Market Data API

- **What it is:** Developer-first brokerage and market-data platform offering commission-free trading (US stocks/ETFs, options, crypto) plus a REST + WebSocket market-data API, with a free paper-trading sandbox.

- **Endpoints / capabilities:**
  - **Market Data API** (base `https://data.alpaca.markets`):
    - **Stocks (`/v2/stocks/`):** historical & latest **bars** (minute/hour/day aggregates), **trades**, **quotes (NBBO)**, **snapshots** (latest trade + quote + minute bar + daily bar + prev daily bar), and **most-actives / movers screener**.
    - **Options (`/v1beta1/options/`):** option **chains**, **bars**, **trades**, **quotes**, **snapshots**, and contract metadata (OCC-format symbols, Greeks/IV on snapshots).
    - **Crypto (`/v1beta3/crypto/`):** **bars**, **trades**, **quotes**, **snapshots**, latest **orderbook** for pairs (BTC/USD, ETH/USD, etc.).
    - **News (`/v1beta1/news/`):** historical + streaming financial news headlines/articles (Benzinga-sourced).
    - **Corporate actions (`/v1/corporate-actions`):** splits, dividends, mergers, spinoffs, name/symbol changes.
    - **Logos (`/v1beta1/logos/`):** company logo images.
    - **Forex / FX rates:** currency rate endpoints (beta).
  - **Trading API** (`https://api.alpaca.markets` / `https://paper-api.alpaca.markets`): orders (market/limit/stop/stop-limit/trailing-stop, bracket/OCO/OTO; bulk cancel), positions, account, account-configuration, portfolio history, activities, watchlists, assets, calendar/clock, options-trading (multi-leg), crypto funding. Also a **Broker API** (BaaS) and an official **MCP server**.

- **Asset classes & coverage:** US equities & ETFs, US equity/index **options**, and **crypto** (spot pairs). **No futures; FX is limited/beta.** Geography: US markets only. **History depth: since 2016 (~7+ years)** for stocks/options; multi-year for crypto.

- **Real-time / delayed / websocket:** REST historical + REST latest endpoints and full **WebSocket streaming** for stocks, options, crypto, and news. Equity real-time sources: **IEX** (free) vs full **SIP/CTA/UTP** consolidated tape (paid). Options: **indicative feed** (free) vs **OPRA** (paid). Crypto real-time is included on all tiers. On the free plan, REST historical for the most recent **15 minutes of SIP data is restricted** (IEX real-time still streams).

- **Auth method:** **API key + secret** via `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` request headers. **OAuth 2.0** is also supported (primarily for Broker/third-party app integrations).

- **Rate limits:** **Basic/free: 200 requests/min.** **Algo Trader Plus: 10,000 requests/min** (marketed as "unlimited"). WebSocket symbol caps — Basic: 30 stock symbols / 200 option quotes; Algo Trader Plus: unlimited stock symbols / 1,000 option quotes. Broker API: tiered 1,000–10,000 rpm.

- **Pricing tiers:**
  - **Basic — $0/month** (default for paper & live accounts).
  - **Algo Trader Plus — $99/month** (full SIP + OPRA real-time, 10,000 rpm, unlimited streaming).
  - **Broker API data add-ons** — tiered ~$0–$2,000/month, with optional **~$1,000/month** for OPRA options access.
  - **Elite** account holders meeting deposit thresholds get Algo Trader Plus data free for personal use.

- **Free tier:** Yes — robust. Free Basic plan includes IEX real-time stocks, indicative options, full crypto, corporate actions, snapshots, aggregate bars, extended hours, ~7+ years history, news, plus unlimited free **paper trading**. Limits: IEX-only equities, 200 rpm, 15-min SIP REST delay, 30-symbol stream cap.

- **SDKs / official client libraries:** **Python** (`alpaca-py`), **JavaScript/Node** (`@alpacahq/alpaca-trade-api`), **Go** (`alpaca-trade-api-go`), **C#/.NET** (`Alpaca.Markets`). Plus an official **MCP server** and a CLI. (Legacy `alpaca-trade-api-python` is deprecated in favor of `alpaca-py`.)

- **Licensing / redistribution notes:** **Redistribution of Alpaca market data is not permitted** — data is for the account holder's own/internal use within their application. Public redistribution, resale, or republishing the raw data to third parties requires a separate agreement (typically via Broker API / sales). ML-training rights are **not explicitly granted** and would fall under the no-redistribution/derived-data restrictions; using data to build models for internal analytics is generally fine, but training models on it for resale is not contemplated by the standard terms — confirm with Alpaca for any commercial/redistribution use case.

- **Role in OUR stack:** Primary **US-equity ingestion + execution** layer. Use historical **bars/trades/quotes** (REST) to build training datasets and backtests for our prediction models; use **WebSocket streaming** (IEX free, or SIP/OPRA via Algo Trader Plus) for live features; pull **corporate actions** to adjust prices, **news** for sentiment signals, and **snapshots/screener** for live dashboards. The **paper-trading** sandbox lets us validate model-driven signals end-to-end before any live execution. Caveat: IEX-only on the free tier means thin volume coverage — budget $99/mo for SIP if we need full-tape accuracy, and treat our own predictions/derived analytics (not raw redistributed quotes) as the user-facing output to stay within the no-redistribution license.

I have enough detail across all three. The Binance Spot REST default limit is well-established (6,000 request weight/min per IP). Let me produce the final markdown block covering all three products densely.

### Crypto Data APIs — Binance, CoinGecko, CoinDesk Data (CryptoCompare/CCData)

- **What it is:** Three complementary crypto market-data sources — Binance (exchange-native REST/WebSocket for its own venue), CoinGecko (cross-market aggregator across 1,500+ exchanges and 18,000+ coins), and CoinDesk Data, formerly CryptoCompare/CCData (institutional aggregator across 300+ exchanges with FCA/EU-BMR benchmark indices).

---

**Binance API**

- **What it is:** Free, exchange-native API exposing Binance's own spot, margin, futures, and options market data plus trading.
- **Endpoints / capabilities:**
  - Prices/aggregates: klines/candlesticks (multiple intervals), 24hr ticker, symbol/book price ticker, rolling-window ticker, average price.
  - Quotes/trades: order book depth snapshots, recent trades, historical trades, aggregate (compressed) trades.
  - Reference/metadata: `exchangeInfo` (symbols, trading rules, filters), server time, system status.
  - Derivatives: USD-M (USDS-margined) and COIN-M futures — mark price, funding rate, open interest + open-interest statistics, long/short ratios, taker buy/sell volume; index price.
  - Options (EAPI): option mark price, open interest, exercise history, Greeks via mark data.
  - WebSocket: market streams (depth diff/partial, trades, aggTrades, klines, ticker, bookTicker, mini-ticker) and WebSocket API (request/response); user-data streams.
  - Bulk/flat-files: `data.binance.vision` daily/monthly CSV dumps (aggTrades, klines, trades) for spot/USD-M/COIN-M; public read-only base `data-api.binance.vision`.
  - No native news/sentiment, corporate actions, or technical-indicator endpoints.
- **Asset classes & coverage:** Crypto only — spot, margin, USD-M & COIN-M perpetual/quarterly futures, European-style options; 300+ assets, thousands of pairs. Global venue (note: Binance.US is a separate API/domain). History: REST limited (klines deep, but OI stats ~1 month); full tick/kline history via flat-file archive.
- **Real-time / delayed / websocket:** Real-time. Public REST + low-latency WebSocket streams and WebSocket API; no artificial delay.
- **Auth method:** Public market data needs no auth (or just an API key). Account/trading endpoints use API key + HMAC-SHA256 (or Ed25519/RSA) signed requests.
- **Rate limits:** IP-weight based, default ~6,000 request-weight/min/IP (spot) plus raw-request and per-account order limits; WebSocket: 1024 streams/connection, 10 inbound msgs/sec; 429s escalate to IP bans of 2 min → 3 days.
- **Pricing tiers:** Free — no paid data tiers. (VIP/fee tiers affect trading costs, not data access.)
- **Free tier:** Entire market-data API is free, including WebSocket and flat-file archives.
- **SDKs / official client libraries:** Official connectors in Python, Node.js, TypeScript, Java, Go, Rust, C#/.NET, PHP, Ruby; Postman & Swagger/OpenAPI specs; popular community libs (python-binance, node-binance-api).
- **Licensing / redistribution notes:** Free for app/personal/commercial use under Binance API Terms; redistribution of raw data and certain commercial reuse is restricted — no explicit ML-training grant; check API terms for downstream distribution.
- **Role in OUR stack:** Primary real-time price/order-book/kline feed and the cheapest source of deep historical OHLCV (via flat-files) for training and live inference on Binance-listed assets; backbone of low-latency websocket ingestion.

---

**CoinGecko API**

- **What it is:** Aggregated cross-exchange crypto market-data API spanning prices, market caps, metadata, and on-chain DEX data.
- **Endpoints / capabilities:**
  - Prices/aggregates: `/simple/price`, `/coins/markets`, `/coins/{id}/market_chart`, OHLC, historical snapshots by date.
  - Quotes/trades & reference: per-exchange tickers, `/coins/{id}` metadata (developer, community, links), contract-address lookup, asset platforms, categories.
  - Exchanges: `/exchanges` list/volume/tickers (1,500+ venues).
  - Derivatives: futures/derivatives tickers and exchanges.
  - Fundamentals/extras: trending, global market metrics, companies' (public treasury) BTC/ETH holdings, search.
  - NFT: collections, floor price, volume.
  - On-chain DEX (GeckoTerminal): token/pool prices, OHLCV, trades across 200+ chains.
  - WebSocket/Webhook: available on Analyst tier and above (real-time price streaming + webhooks).
  - No first-party news/sentiment, corporate actions, or technical-indicator endpoints.
- **Asset classes & coverage:** Crypto only — 18,000+ coins/tokens, 1,500+ exchanges, NFTs, on-chain DEX across 200+ blockchains, derivatives. Global. History: 2 yrs (Basic) up to 10 yrs (Analyst/Lite/Enterprise).
- **Real-time / delayed / websocket:** Demo ~60s freshness; paid down to real-time; WebSocket + webhooks from Analyst tier up.
- **Auth method:** API key via header (`x-cg-pro-api-key` / `x-cg-demo-api-key`).
- **Rate limits:** Demo 100 calls/min (10k credits/mo); Basic 300/min (100k/mo); Analyst 500/min (500k/mo); Lite 500/min (2M/mo); Enterprise custom. Limited per key + IP.
- **Pricing tiers (current):** Demo $0; Basic $35/mo ($29 annual); Analyst $129/mo ($103 annual); Lite $499/mo ($399 annual); Enterprise custom. Overage ~$0.0005/call where enabled.
- **Free tier:** Demo plan — $0, 10k credits/mo, 100 calls/min, 50+ endpoints, 1 key, commercial use with attribution; no credit card.
- **SDKs / official client libraries:** Official TypeScript and Python SDKs; OpenAPI spec; many community libraries.
- **Licensing / redistribution notes:** Paid tiers carry a commercial license (attribution required on Demo/Basic). Redistribution, white-labeling, and broad data resale require Enterprise/custom licensing; ML-training rights not granted by default — negotiate under Enterprise.
- **Role in OUR stack:** Cross-asset reference and fundamentals layer — normalized coin IDs/metadata, market-cap/category context, multi-exchange aggregated prices, and 10-yr history for feature engineering and backfilling assets not on Binance.

---

**CoinDesk Data API (formerly CryptoCompare / CCData)**

- **What it is:** Institutional-grade aggregated crypto data API with tick-level CEX data, AMM/DEX swaps, and regulated benchmark indices.
- **Endpoints / capabilities:**
  - Prices/aggregates: CCCAGG aggregated prices, multi-interval OHLCV (daily/hourly/minute), historical day/hour/minute series.
  - Quotes/trades: tick-level trades (nanosecond timestamps, CCSEQ sequencing for gapless reconstruction), Level-2 order-book snapshots.
  - Spot, Futures, Options: dedicated instrument families with real-time + historical data from leading derivatives/options exchanges (incl. open interest, funding).
  - Indices & reference rates: FCA-regulated, EU-BMR-compliant benchmarks; programmatic instrument/exchange discovery.
  - On-chain: DEX/AMM swap tracking, blockchain/network data, asset metadata.
  - News & sentiment: aggregated crypto news feed and social/sentiment data.
  - WebSocket/streaming: 10+ real-time channels (trades, order book, indices).
  - 200+ REST endpoints total; deep historical archive (10+ yrs, tick granularity).
  - No traditional-equity corporate actions (crypto-focused).
- **Asset classes & coverage:** Crypto only — 7,000+ assets, 300+ exchanges (CEX + DEX), spot/futures/options/indices/on-chain. Global. History: 10+ years, tick-level.
- **Real-time / delayed / websocket:** Real-time REST + 10+ WebSocket channels; full historical replay.
- **Auth method:** API key (passed as header or query param).
- **Rate limits:** Tier-dependent (per-second/minute/month quotas); free tier historically capped at a 250k lifetime-call limit (non-resetting).
- **Pricing tiers:** Not publicly listed — Start-Up through Enterprise all require a sales conversation; custom quotes.
- **Free tier:** Being retired — as of ~May 21, 2026 accounts without a subscription lose API access (legacy 250k lifetime-call free tier discontinued).
- **SDKs / official client libraries:** REST/WebSocket documented with examples; historically Python and JS/community wrappers; Postman/OpenAPI; no broad first-party multi-language SDK suite emphasized.
- **Licensing / redistribution notes:** Commercial/redistribution terms negotiated per contract; regulated indices carry benchmark-licensing requirements; redistribution, display, and ML-training rights are explicitly licensed deal-by-deal (institutional licensing model).
- **Role in OUR stack:** Premium/institutional augmentation — regulated benchmark reference rates, cross-venue tick history for high-fidelity backtesting, plus the only one of the three offering native news/sentiment signals to feed prediction features; reserved for production/commercial tiers given paid-only access post-2026.
### Forex Data APIs — OANDA v20 API · Twelve Data Forex · Polygon.io / Massive Forex

- **OANDA v20 REST & Streaming API**
  - **What it is:** Broker-native trading + pricing API for OANDA's v20 trading engine, primarily built for execution but widely used as a free, broker-grade FX market-data source.
  - **Endpoints / capabilities:** Six endpoint groups — **Account** (details, summary, tradeable instruments, configuration), **Instrument** (candles/OHLC, order book, position book), **Pricing** (latest prices for a list of instruments, candles-latest), **Order** (create/modify/cancel market, limit, stop, take-profit, stop-loss, trailing), **Trade** (open trades, close, modify), **Position** (open/closed positions, close), and **Transaction** (time-/ID-ranged transaction history, single transaction). No native news/sentiment, fundamentals, corporate actions, or technical-indicator endpoints — it is execution + raw pricing only.
  - **Asset classes & coverage:** FX (major/minor/exotic pairs) plus CFDs offered by OANDA (metals, indices, commodities, bonds — varies by division/region). ~70–120+ tradeable instruments depending on account/division. Historical candles back to ~2005.
  - **Real-time / delayed / websocket:** Real-time 24/5 FX. Dedicated **streaming** hosts for price stream and transaction stream (HTTP chunked streaming, not classic WebSocket). Practice (`api-fxpractice`/`stream-fxpractice`) and live (`api-fxtrade`/`stream-fxtrade`) environments.
  - **Auth method:** Personal access (bearer) token generated in the OANDA Account Management Portal; sent as `Authorization: Bearer <token>`. No OAuth.
  - **Rate limits:** 120 REST requests/sec per IP (HTTP 429 on excess); max 20 active streams per IP; max 2 new connections/sec; persistent connections recommended.
  - **Pricing tiers:** No paid data-API tiers — API access is included free with any OANDA account (demo or live). Revenue is from trading spreads/commissions, not data. (OANDA's separate **Exchange Rates Data API** product is a distinct paid offering starting ~$4,850/yr; not the v20 API.)
  - **Free tier:** Yes — fully free with a (no-funding-required) practice account; live data requires a funded live account but the API itself is free.
  - **SDKs / official client libraries:** No first-party SDK; de-facto standard is the community `oandapyV20` (Python). Community wrappers exist for Go, JavaScript/Node, Java, R, C#.
  - **Licensing / redistribution notes:** Data tied to account use; practice/demo data is not licensed for commercial redistribution, and live-data redistribution is restricted under OANDA's terms — treat as **internal use**. No explicit ML-training grant; clear with OANDA for any redistribution or productization.
  - **Role in OUR stack:** Use as a low-cost, broker-grade real-time + historical FX feed (intraday candles, live bid/ask via price stream) to power feature engineering and the FX slice of a stock-analysis/prediction API. Best as a streaming ingest source and optional execution leg — not as the source of fundamentals, news, or equities data.

- **Twelve Data Forex API**
  - **What it is:** Unified market-data REST + WebSocket API where FX is one asset class alongside stocks/ETFs/crypto; credit-metered.
  - **Endpoints / capabilities:** **Time Series** (OHLCV, many intervals), **Real-time Price**, **Quote**, **Exchange Rate**, **Currency Conversion**, **Time Series Cross** (computed cross-rates for exotic pairs), 100+ **Technical Indicators** (SMA, EMA, RSI, MACD, BBANDS, etc. server-side), **Reference/metadata** (forex_pairs, symbol_search, earliest_timestamp), **Batch** requests (up to 120 symbols/call), and **WebSocket** streaming. Equities side adds fundamentals/news, but FX-relevant set is prices/quotes/indicators/reference.
  - **Asset classes & coverage:** FX + metals — 140 currencies/metals forming 2,000+ pairs (majors, minors, exotics, XAU/XAG/oil). Plus stocks, ETFs, crypto, indices, funds/fixed income on higher tiers. 1M+ instruments, 50+ countries. History 20+ years (available even on free).
  - **Real-time / delayed / websocket:** FX updates at least once/min (sub-minute on better feeds). Free tier is real-time for FX/crypto/US equities but capped; WebSocket streaming available on **Pro and above**.
  - **Auth method:** API key (query param or header). No OAuth.
  - **Rate limits:** Credit-based per minute. Free ~8 credits/min (800/day cap); Grow ~377/min; Pro ~610–1,597/min; Ultra ~2,584–10,946/min. WebSocket credits = 1 per subscribed symbol (Pro ~500–1,500, Ultra ~2,500–10,000).
  - **Pricing tiers:** Free $0; Grow ~$79/mo ($66 annual); Pro from ~$99–$229/mo; Ultra from ~$329–$999/mo; Business plans (Venture/Enterprise) custom with SLA. (Exact figures vary by credit bundle selected.)
  - **Free tier:** Yes — $0/mo, ~8 credits/min (800 requests/day), real-time FX/crypto/US equities, full 20+ yr history, indicators, 8 trial WebSocket credits.
  - **SDKs / official client libraries:** Official clients in Python, plus C++, C#, Java, R.
  - **Licensing / redistribution notes:** Usage and redistribution rights defined per data add-on and tier (personal vs commercial vs redistribution), aligned to exchange/vendor terms; redistribution and ML-training scope must be confirmed with sales — base plans are effectively internal/commercial-use.
  - **Role in OUR stack:** Strong single-vendor option — one key for FX time series + indicators + cross-rates plus equities/crypto, simplifying ingestion. Use its server-side indicators and Time Series Cross for fast feature generation, and WebSocket (Pro+) for live FX features feeding the prediction model.

- **Polygon.io Forex API (now Massive — "Currencies")**
  - **What it is:** Developer-first market-data API; Polygon rebranded to **Massive** in early 2026, with FX bundled under the "Currencies" (Forex + Crypto) asset class.
  - **Endpoints / capabilities:** **Aggregates** (custom OHLC bars, grouped daily, previous close), **Quotes / Last Quote** (best bid/ask), **Real-time Currency Conversion**, **Snapshots** (all tickers / single / gainers-losers), **Technical Indicators** (SMA, EMA, MACD, RSI server-side), **Reference/Tickers** metadata, **Market Status/holidays**, **Flat Files** (bulk historical via S3), and **WebSocket** channels: per-minute aggregates, per-second aggregates, quotes, and **Fair Market Value (FMV)** (Business tier).
  - **Asset classes & coverage:** FX — 1,000+ currency pairs; plus crypto in the same Currencies bundle, and separate ladders for Stocks, Options, Indices, Futures. History up to ~20 years on top tiers (tiered: ~2/5/10/20 yr).
  - **Real-time / delayed / websocket:** Lower paid tier ~15-min delayed; real-time + WebSocket on higher tiers; per-second WS bars and FMV at the top. Flat files for bulk historical backfill.
  - **Auth method:** API key (bearer/`apiKey`). No OAuth.
  - **Rate limits:** Free ~5 calls/min; paid premium tiers advertise **unlimited** API calls / no per-minute cap (FX/crypto often marketed as unlimited streaming + requests).
  - **Pricing tiers:** Per-asset-class ladder (verify on massive.com post-rebrand): Basic free $0; Starter ~$29/mo; Developer ~$79/mo; Advanced ~$199/mo; Business custom. Currencies bundle frequently surfaces an unlimited entry near ~$49/mo.
  - **Free tier:** Yes — Basic $0, ~5 calls/min, end-of-day, ~2-yr history, no real-time/WebSocket.
  - **SDKs / official client libraries:** Official clients in Python, JavaScript/TypeScript, Go, with community wrappers (`polygon` Python lib, etc.).
  - **Licensing / redistribution notes:** Standard plans are internal/commercial use; **redistribution rights and ML-training/derivative use require the Business plan** and explicit licensing. FMV and certain real-time feeds are Business-gated.
  - **Role in OUR stack:** Best as a clean, scalable historical + real-time FX engine — use Flat Files (S3) for bulk model training data and WebSocket per-second/per-minute bars for live features. Its multi-asset ladder lets us add equities/options/crypto from the same vendor when the stock-prediction API expands; reserve the Business tier if we ever redistribute outputs or train on the data at scale.

### 5.2 Modeling, Forecasting & Infra Frameworks

I now have all the information needed to produce the dense, factual markdown block.

### Nixtla (TimeGPT + StatsForecast / MLForecast / NeuralForecast)

- **What it is:** A time-series forecasting and anomaly-detection platform combining the hosted/proprietary TimeGPT foundation model (zero-shot, generative pretrained transformer trained on 100B+ data points) with a free, open-source forecasting ecosystem ("Nixtlaverse") — StatsForecast, MLForecast, NeuralForecast, HierarchicalForecast, plus utilsforecast/coreforecast.

- **Endpoints / capabilities (TimeGPT API + SDK):**
  - **Forecast** — point and probabilistic forecasts; `level` prediction intervals (0–100) and `quantiles`; models `timegpt-1` (default) and `timegpt-1-long-horizon`.
  - **Historical anomaly detection** — `detect_anomalies`; flags values outside a confidence interval (default 99%), adjustable via `level`.
  - **Online / real-time anomaly detection** — streaming/continuous detection with adjustable sensitivity.
  - **Cross-validation** — `cross_validation` over multiple rolling windows (`n_windows`, `step_size`, refit).
  - **Fine-tuning** — `finetune` / `finetuned_models` / `finetuned_model` / `delete_finetuned_model`; `finetune_steps`, `finetune_depth`, `finetune_loss`; persistent fine-tuned model IDs.
  - **Exogenous & feature engineering** — future exogenous (`X_df`), historical exogenous (`hist_exog_list`), `clean_ex_first`, automatic/custom `date_features`, one-hot encoding; what-if / scenario planning; explainability & driver analysis.
  - **Utility** — `usage()` (consumed requests & limits), `validate_api_key()`, `plot()` (matplotlib/plotly).
  - No financial-data reference/fundamentals/news/options/corporate-actions endpoints — this is a forecasting engine, not a market-data provider (you supply the time series).

- **Asset classes & coverage:** Domain-agnostic — not a market data feed. Trained across retail, electricity/energy, finance, IoT, healthcare, weather, web traffic, banking, demographics. Handles any frequency (sub-hourly to yearly), irregular timestamps, multiple/millions of series, and multivariate inputs. History depth and geography are whatever the user provides.

- **Real-time / delayed / websocket:** No market-data delay concept. Supports batch and online/real-time anomaly detection on streaming data; GPU inference ~0.6 ms/series. No public websocket market feed (you connect your own warehouses/lakehouses/files/streams).

- **Auth method:** API key — `api_key` arg to `NixtlaClient` or `NIXTLA_API_KEY` env var (key issued at nixtla.io). Enterprise adds SSO, RBAC, audit logging.

- **Rate limits:** Per-tier API-call limits, customizable on Enterprise plans (call quotas, user seats, support level). Programmatic `usage()` returns consumed requests and current limits. No fixed public per-second figures published.

- **Pricing tiers:** Open-source libraries — free forever. TimeGPT — 30-day free trial (no credit card), then **custom Enterprise plans** (pricing on request via support@nixtla.io / sales). No fixed public dollar figures; plans customize API-call limits, seats, and support (email/chat/phone/dedicated). Deployment options: Nixtla Cloud (managed), self-hosted/BYOC (Docker, Pip, Terraform on AWS/GCP/Azure), and in-warehouse (Snowflake stored procs/UDTFs, Databricks).

- **Free tier:** Full open-source stack is free (Apache 2.0). TimeGPT: 30-day free trial only — no perpetual free API tier.

- **SDKs / official client libraries:** Python (`nixtla` PyPI package, primary), R (`nixtlar`); REST API with additional language clients noted (JavaScript, Go). Open-source libs are Python with pandas/polars/Spark/Dask/Ray backends. Distributed execution supported across Spark/Dask/Ray.

- **Licensing / redistribution notes:** Open-source libraries (StatsForecast, MLForecast, NeuralForecast, HierarchicalForecast, utils/core) and the TimeGPT **SDK** are **Apache 2.0** (permissive — commercial use, redistribution, and modification allowed). The **TimeGPT model itself is closed-source/proprietary**, accessed only via API or licensed self-host/BYOC deployment — model weights are not redistributable. Enterprise: SOC 2 Type II, GDPR/HIPAA, zero data egress / zero data retention, VPC/BYOC. ML-training rights on the proprietary model are governed by the commercial agreement, not the Apache SDK license.

- **Role in OUR stack:** This is the prediction layer, not a data source. We feed our own ingested OHLCV/price history (from a market-data vendor) into either (a) the open-source StatsForecast/MLForecast/NeuralForecast libs — self-hosted, free, no per-call cost, full control (AutoARIMA/ETS/Theta baselines, gradient-boosted ML with engineered features, or neural models like NHITS/PatchTST/TFT for deep multivariate forecasts with exogenous market signals) — or (b) the hosted TimeGPT API for fast zero-shot/fine-tuned forecasts plus prediction intervals and built-in anomaly detection (regime/outlier flags on price or volume) without training infrastructure. Practical pattern: open-source libs for reproducible, cost-free production forecasting and backtesting (cross_validation), with TimeGPT as a strong zero-shot benchmark or premium tier; exogenous variables let us inject features (calendar effects, sentiment, macro series) to improve directional stock predictions.

I now have enough authoritative detail on both models. These are open-source ML models (not market-data APIs), so I'll adapt the requested schema to fit how foundation models actually work — covering capabilities, deployment, licensing, and role in a stock-prediction stack.

### Amazon Chronos-2 & Google TimesFM 2.5 (Forecasting Foundation Models)

- **What it is**
  - Two open-source, pretrained time-series foundation models that produce zero-shot probabilistic forecasts (point + quantiles) for arbitrary numeric series — distributed as model weights/SDKs, not hosted market-data REST APIs.

- **Endpoints / capabilities** (these are libraries + managed endpoints, not data APIs)
  - **Chronos-2:** `Chronos2Pipeline.from_pretrained()` Python API; `predict_df()` DataFrame in/out; configurable quantile levels (e.g. [0.1, 0.5, 0.9]); zero-shot **univariate, multivariate (joint/coevolving series), and covariate-informed** forecasting (past + known-future covariates via `future_df`); in-context learning (no retraining); group-attention for efficiency (>300 series/sec on one A10G). Productionizable via **AutoGluon-Cloud** (real-time/serverless/batch), **SageMaker JumpStart** (CPU/GPU real-time endpoints), and direct local inference.
  - **TimesFM 2.5:** `TimesFM_2p5_200M_torch.from_pretrained(...)` + `model.compile(ForecastConfig(...))` then `forecast(horizon, inputs)`; continuous quantile head (optional 30M params, up to 1k-step horizon); covariate support via **XReg**; **LoRA fine-tuning** (HF Transformers + PEFT). Managed surfaces: **BigQuery ML `AI.FORECAST` / built-in TimesFM model**, **AlloyDB**, **Google Sheets** function, **Vertex AI Model Garden** Dockerized endpoint. Ships a **SKILL.md** (machine-readable agent skill, MCP/Claude-Code compatible, since Mar 2026).

- **Asset classes & coverage**
  - Domain-agnostic — **no built-in market data or asset coverage**. They forecast whatever numeric history you feed them (equities, ETFs, FX, crypto, futures, options-implied vols, macro). Geography/history depth = whatever you supply. Context windows: **Chronos-2 ~8,192 tokens; TimesFM 2.5 up to 16,384 timesteps** (~44 yrs daily / 333 yrs weekly).

- **Real-time / delayed / websocket**
  - No streaming or quote feeds — they are inference engines. "Real-time" = low-latency synchronous inference (AutoGluon-Cloud/SageMaker real-time endpoints; Vertex AI endpoint; BigQuery on-demand SQL). Batch/serverless inference also supported. Latency is yours to manage; data must be sourced separately.

- **Auth method**
  - Self-hosted weights: none (local). Managed: standard cloud IAM — **AWS SigV4/IAM** (SageMaker, AutoGluon-Cloud) for Chronos-2; **Google Cloud IAM / OAuth service accounts** (BigQuery, Vertex AI) for TimesFM. Hugging Face token to pull weights.

- **Rate limits**
  - No API rate limits when self-hosted (bounded only by GPU/CPU throughput, e.g. Chronos-2 >300 series/s on A10G). Managed limits follow the host's quotas (SageMaker endpoint concurrency; Vertex AI / BigQuery ML query & slot quotas).

- **Pricing tiers**
  - **Weights are free** (Apache-2.0). You pay only for compute: SageMaker/Vertex AI endpoint instance-hours; **BigQuery ML `AI.FORECAST`** billed at BigQuery ML on-demand evaluation/inspection/prediction rates (bytes/slots); AutoGluon-Cloud on underlying AWS compute. No per-call SaaS license fee.

- **Free tier**
  - Fully free for local/self-hosted use (download + run on your own CPU/GPU). Cloud "free tier" is just the provider's standard free credits/quotas.

- **SDKs / official client libraries**
  - **Chronos-2:** Python `pip install chronos-forecasting`; AutoGluon TimeSeries; SageMaker SDK. **TimesFM:** Python `pip install timesfm[torch]` / `[flax]` / `[xreg]` (PyTorch + Flax/JAX backends); BigQuery SQL; Google Sheets; Vertex AI SDK. Both publish weights on Hugging Face. Primarily Python-centric.

- **Licensing / redistribution notes**
  - **Both Apache-2.0** — permits commercial use, modification, redistribution, and **fine-tuning/ML-training of derivatives** with attribution and license inclusion. No internal-use-only restriction; safe to embed in a commercial product. Caveat: this covers the *model*, not any market data you run through it (data vendor terms still govern that).

- **Role in OUR stack**
  - These are the **forecasting brain**, not a data source. Pipeline: ingest OHLCV/fundamentals/sentiment from a real market-data API (e.g. Polygon/Finnhub) → engineer features and known-future covariates (earnings dates, calendar, macro) → feed history + covariates to the model for **zero-shot probabilistic price/return forecasts with quantile bands** (confidence intervals for risk display). Use **Chronos-2** for joint multivariate forecasting across correlated tickers/sectors and easy covariate ensembling via AutoGluon; use **TimesFM 2.5** when you want very long context (16k), serverless **BigQuery `AI.FORECAST`** SQL inference next to stored data, or LoRA fine-tuning on your own history. Self-host on a GPU for cost control, or hit managed endpoints; Apache-2.0 means we can fine-tune and ship predictions commercially without redistribution constraints.

Sources:
- [Introducing Chronos-2 — Amazon Science](https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting)
- [amazon-science/chronos-forecasting (GitHub)](https://github.com/amazon-science/chronos-forecasting)
- [amazon/chronos-2 (Hugging Face)](https://huggingface.co/amazon/chronos-2)
- [AutoGluon Chronos-2 tutorial](https://auto.gluon.ai/dev/tutorials/timeseries/forecasting-chronos.html)
- [google-research/timesfm (GitHub)](https://github.com/google-research/timesfm)
- [TimesFM 2.5 guide — explainx.ai](https://www.explainx.ai/blog/google-timesfm-2-5-time-series-foundation-model-2026)
- [The TimesFM model — BigQuery docs](https://docs.cloud.google.com/bigquery/docs/timesfm-model)
- [AI.FORECAST function — BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-ai-forecast)
- [TimesFM in BigQuery and AlloyDB — Google Cloud Blog](https://cloud.google.com/blog/products/data-analytics/timesfm-models-in-bigquery-and-alloydb)

> These are open-source modeling libraries (not commercial data APIs), so the schema below is adapted: capabilities → model families/APIs; coverage → input data handled; pricing → open-source/free; auth/limits → N/A for self-hosted libraries.

### Modeling Libraries (Darts, sktime, statsmodels, pmdarima, XGBoost/LightGBM/CatBoost, PyTorch Forecasting, Prophet/NeuralProphet)

- **What it is**: A suite of open-source Python time-series & ML modeling libraries — installed and run in-process (not hosted APIs) — used to fit/forecast/classify on data we ingest from external market-data vendors.

- **Endpoints / capabilities** (per library; "endpoints" = model classes & APIs):
  - **Darts** (v0.45.0, Jun 2026): single unified `fit()/predict()/backtest()/historical_forecasts()` API. Statistical: ARIMA, AutoARIMA, AutoETS, VARIMA, ExponentialSmoothing, Theta/FourTheta, TBATS, Croston, FFT, KalmanForecaster, Prophet wrapper. Regression/ML: LinearRegression, RandomForest, `XGBModel`, `LightGBMModel`, `CatBoostModel`. Deep learning (PyTorch-Lightning): RNN/LSTM/GRU, BlockRNN, NBEATS, NHiTS, TCN, Transformer, TFT, DLinear, NLinear, TiDE, TSMixer, NeuralForecast wrapper. Foundation/zero-shot: Chronos2, TimesFM2.5, TiRex, PatchTSTFM. Ensembles & conformal (ConformalNaive/ConformalQR for prediction intervals). Anomaly detection (KMeansScorer, QuantileDetector, PyOD integration). Past/future/static covariates; probabilistic forecasting; data transformers; metrics; classification models.
  - **sktime** (v0.40.x, 2026): scikit-learn-style unified interface across forecasting, classification, regression, clustering, segmentation, transformation, annotation. Reduction (turn regressors into forecasters), pipelines, hierarchical/global forecasting, AutoML/tuning (grid/random/AutoARIMA via wrappers), 200+ estimators, adapters to statsmodels/pmdarima/Prophet/Darts-style backends.
  - **statsmodels** (v0.14.6 stable / 0.15 dev, 2026): `tsa` + state-space (`statespace`): ARIMA, SARIMAX, VARMAX, VAR, VECM, UnobservedComponents, DynamicFactor, ExponentialSmoothing/ETS, Markov-switching, GARCH-adjacent tools; residual diagnostics, ACF/PACF, stationarity tests (ADF/KPSS), cointegration, impulse responses, simulation, the "news"/forecast-update API, full statistical inference (CIs, p-values).
  - **pmdarima** (v2.1.1, Nov 2025): `auto_arima` (R-equivalent stepwise/grid order selection), Pipeline, preprocessing transformers (BoxCox, Fourier featurizer), CV utilities; thin Cython wrapper over statsmodels SARIMAX.
  - **XGBoost** (v3.2.0, Feb 2026): gradient-boosted trees — regression (incl. quantile/pinball), classification, ranking; sklearn API, `DMatrix`, GPU `hist` (TMA-optimized, device-memory model storage), categorical re-coder, SHAP, distributed (Dask/Spark/Ray), early stopping.
  - **LightGBM** (v4.6, 2026): leaf-wise histogram boosting, GOSS, EFB; regression/classification/ranking, native categoricals, GPU/CUDA, distributed, sklearn API.
  - **CatBoost** (v1.2.x, 2026): ordered boosting, best-in-class native categorical handling, GPU training, regression/classification/ranking, SHAP, uncertainty estimation, text/embedding features.
  - **PyTorch Forecasting** (sktime org, 2026): high-level deep TS API on PyTorch-Lightning — TFT (interpretable, variable-selection/attention), N-BEATS, N-HiTS, DeepAR (probabilistic), DecoderMLP, RecurrentNetwork; `TimeSeriesDataSet`, multi-horizon, quantile losses, GPU scaling, auto logging.
  - **Prophet** (Meta, 2026): additive trend+seasonality+holidays decomposition, changepoint detection, regressors, MCMC/MAP uncertainty intervals; `fit/predict/plot_components`.
  - **NeuralProphet** (community, PyTorch): Prophet-style decomposable model + AR-Net autoregression, lagged & future regressors, neural seasonality, local context; interpretable components.

- **Asset classes & coverage**: Library-agnostic — any numeric/categorical time series we supply (equities, ETFs, FX, crypto, futures, options, fundamentals). No built-in geographies or history depth; coverage is entirely determined by the data feed we pipe in. Darts/sktime/PyTorch-Forecasting natively handle univariate, multivariate, multiple/global series and exogenous covariates; statsmodels/pmdarima/Prophet are primarily univariate (+ exog regressors); XGBoost/LightGBM/CatBoost operate on flat tabular feature matrices (require manual lag/window feature engineering).

- **Real-time / delayed / websocket**: N/A — these are batch/in-process compute libraries with no streaming endpoints. "Real-time" inference is local: trained models call `predict()` on incoming bars; tree models (XGB/LGBM/CatBoost) and fitted classical models give sub-millisecond inference; deep models run on GPU/CPU. Online/incremental refit is partial (Prophet/ARIMA refit cheaply; trees support continued training).

- **Auth method**: None — self-hosted open-source packages; no API key/OAuth.

- **Rate limits**: None (limited only by local CPU/GPU/RAM).

- **Pricing tiers**: Free / $0 — all are free open-source software; only cost is compute (your servers/GPUs).

- **Free tier**: Entire functionality is free; no paid tiers, no metering.

- **SDKs / official client libraries**: Python is the primary interface for all. XGBoost, LightGBM, CatBoost additionally ship official **R, C/C++ APIs**, plus **Java/Scala (JVM)**, and CatBoost has **C++/Java/Node/.NET/Rust** inference bindings; XGBoost/LightGBM have JVM + Spark packages. statsmodels/Darts/sktime/pmdarima/PyTorch-Forecasting/Prophet/NeuralProphet are Python-only (Prophet also has an R package; statsmodels has Stata-comparable APIs).

- **Licensing / redistribution notes**: Permissive — **Darts (Apache-2.0)**, **sktime (BSD-3)**, **statsmodels (BSD-3)**, **pmdarima (MIT)**, **XGBoost (Apache-2.0)**, **LightGBM (MIT)**, **CatBoost (Apache-2.0)**, **PyTorch Forecasting (MIT)**, **Prophet (MIT)**, **NeuralProphet (MIT)**. All allow unrestricted commercial/internal use, model-training, and embedding in a closed-source product; no redistribution restrictions on outputs and no ML-training-rights limits (data-licensing constraints come from the vendor feed, not these libraries). Note: foundation models bundled via Darts wrappers (Chronos, TimesFM, TiRex) carry their own model-weight licenses to check separately.

- **Role in OUR stack**: This is a reference catalog, not the committed runtime portfolio. The committed stack stays deliberately lean: **StatsForecast + statsmodels** for baselines, **LightGBM** for tabular feature models, **Chronos-2** for zero-shot/few-shot probabilistic forecasts, and owned evaluation/indicator code. Darts, NeuralForecast/PyTorch Forecasting, pmdarima, XGBoost, CatBoost, Prophet/NeuralProphet, and sktime remain comparison or migration candidates only; promote one only after it beats the lean stack under the same walk-forward evaluation and its operational/license cost is justified.
### Serving, Data & Infrastructure Tooling

#### API & Web Layer
- **FastAPI** — modern async Python web framework for building HTTP/JSON APIs with type-hint-driven validation. · **Role in our stack:** primary REST layer exposing analysis and prediction endpoints (e.g. `/quote`, `/indicators`, `/predict`). · **Key features:** automatic OpenAPI/Swagger + ReDoc docs, native `async`/`await` for concurrent I/O, dependency-injection system, Pydantic-based request/response models.
- **Uvicorn** — lightning-fast ASGI server built on uvloop and httptools. · **Role in our stack:** runs the FastAPI app as the ASGI worker process (one or more per container). · **Key features:** ASGI 3.0 support, HTTP/1.1 + WebSockets, hot-reload in dev, `--workers` and graceful shutdown.
- **Gunicorn (not adopted)** — battle-tested WSGI/process manager historically used as a production worker supervisor. · **Role in our stack:** not adopted day one; the deprecated `uvicorn.workers.UvicornWorker` path is avoided. We run Uvicorn workers directly under Compose/system supervision and revisit a dedicated worker supervisor only if we need lifecycle controls Uvicorn does not provide. · **Key features:** pre-fork worker model, worker lifecycle/health management, configurable timeouts and graceful restarts, signal-based zero-downtime reloads.
- **Pydantic v2** — data-validation and settings library with a Rust core (`pydantic-core`). · **Role in our stack:** defines request/response schemas, DTOs, and typed config (`BaseSettings`) shared across the API and pipelines. · **Key features:** 5–50x faster validation than v1, strict/lax modes, `model_validate`/`model_dump` serialization, `Annotated`-based custom validators and `pydantic-settings` env loading.
- **httpx** — fully featured HTTP client supporting both sync and async. · **Role in our stack:** outbound calls to market-data vendors and external APIs, plus in-process FastAPI test client. · **Key features:** `async` client with connection pooling, HTTP/2, timeouts and retries (via transport), drop-in `requests`-like API.
- **slowapi** — rate-limiting extension for Starlette/FastAPI built on the `limits` library. · **Role in our stack:** per-IP/per-API-key throttling on public and prediction endpoints to protect data quotas and compute. · **Key features:** decorator and middleware limits (e.g. `@limiter.limit("60/minute")`), Redis-backed shared counters, `429` responses with `Retry-After`, fixed/moving-window strategies.
- **PyJWT + bcrypt** — JWT handling plus password/key hashing primitives for future auth flows. · **Role in our stack:** PyJWT is the committed JWT library; bcrypt is available for hashed secrets/API-key material when self-serve key management lands. · **Key features:** HS256/RS256 signing and verification, `exp`/`iat`/`aud` claim validation, maintained small surface area.

#### Data Stores & Caching
- **PostgreSQL** — robust open-source relational database and the system of record. · **Role in our stack:** stores symbols, users/API keys, model metadata, predictions, and reference data. · **Key features:** ACID transactions, rich JSONB support, window functions and CTEs, mature indexing (B-tree/GIN/BRIN) and replication.
- **TimescaleDB** — PostgreSQL extension purpose-built for time-series at scale. · **Role in our stack:** stores OHLCV bars and tick/quote history as hypertables, serving fast range queries and pre-rolled aggregates to the API and feature pipeline. · **Key features:** hypertables with automatic time/space chunk partitioning and chunk pruning; continuous aggregates (self-refreshing materialized rollups like 1m→1h→1d); hypercore columnstore compression (~90–95% reduction on cold chunks) with transparent decompression; data-retention and tiering policies.
- **Redis** — in-memory key-value store used for caching, messaging, and counters. · **Role in our stack:** split into two instances with different failure modes: `redis-cache` caches hot quotes/indicator results and holds slowapi rate-limit counters under LRU eviction; `redis-celery` is the durable Celery broker/result backend with AOF and `noeviction`. · **Key features:** sub-millisecond reads with TTL expiry, pub/sub and list/stream primitives for queues, atomic `INCR`/`INCRBY` for limiters, optional persistence (RDB/AOF) and Lua scripting.

#### Ingestion & Orchestration
- **Celery + Beat (committed)** — distributed task queue with a periodic scheduler. · **Role in our stack:** the single, committed orchestration system (see §4). Celery Beat runs scheduled ingestion (EOD fundamentals, intraday bar polling, news sweeps) while Celery workers run async jobs, heavy feature computation, backtests, and per-symbol prediction fan-out — all over the Redis broker already in the stack. · **Key features:** horizontal worker scaling, `beat` cron-style periodic tasks, retries/acks-late, rate-limit-aware backoff, idempotent upserts, and task routing/priorities. **Why over Prefect:** Celery already has to exist for off-request-thread compute, so we run one scheduler, not two.
- **Prefect (deferred migration target — NOT adopted day one)** — modern Python-native dataflow orchestrator. · **Role in our stack:** the migration target *if and only if* DAG-style data lineage, complex multi-stage backfills, or a richer observability UI become a real requirement; we do not pay for a second scheduler on day one. · **Key features:** `@flow`/`@task` decorators with retries/caching, dynamic/parameterized runs, observability UI with run history and logs. **When to revisit:** cross-dataset dependency graphs and lineage/observability needs Celery + Beat cannot express cleanly.
- **APScheduler (not used)** — lightweight in-process job scheduler. · **Role in our stack:** not adopted; noted only as the minimal single-node alternative — Celery Beat already covers scheduled jobs without adding a second system. · **Key features:** cron/interval/date triggers, persistent job stores, embedded in-process scheduling, no broker required.

#### ML Lifecycle & Serving
- **MLflow** — open-source platform for experiment tracking and model management. · **Role in our stack:** logs every training run (params/metrics/artifacts) and governs promoted forecasting models in the registry. · **Key features:** experiment tracking with autologging, model registry with versioning and aliases (e.g. `@champion`/`@challenger`), full run-to-model lineage for reproducibility, `mlflow.search_logged_models()` and pluggable artifact/backing stores.
- **Feast (optional — adopt only when online/offline feature skew becomes a real pain)** — open-source feature store for ML. · **Role in our stack:** deferred per §4; when adopted, defines and serves engineered indicators/features consistently for both training and low-latency online prediction. · **Key features:** offline store (e.g. Postgres/TimescaleDB) for historical training data, online store (e.g. Redis) for real-time serving, point-in-time-correct joins that prevent label leakage, declarative feature views and registry.
- **BentoML (optional escape hatch — for scaled/GPU serving separate from the API tier)** — framework for packaging and serving ML models as production services. · **Role in our stack:** deferred per §4; the escape hatch when models need independent scaling, adaptive batching, or GPU-backed servers — wraps trained predictors into versioned, containerizable inference services callable from FastAPI or deployed standalone. · **Key features:** "Bento" artifact packaging with dependencies, adaptive request batching, multi-framework runners, auto-generated OpenAPI service and OCI image builds.
- **ONNX Runtime (optional)** — cross-platform, optimized inference engine for ONNX-format models. · **Role in our stack:** optional path to export trained models to ONNX for faster, framework-agnostic, lower-latency CPU/GPU inference. · **Key features:** graph optimizations and operator fusion, execution providers (CPU/CUDA/TensorRT), quantization for smaller/faster models, portable single-format deployment.

#### Indicators & Backtesting
- **Owned indicator functions (committed)** — a small audited feature library built in-house over pandas/numpy. · **Role in our stack:** compute only the technical indicators we actually use, with golden-value tests and clear adjustment/freshness rules. · **Key features:** tiny dependency surface, transparent formulas, unit-testable edge cases.
- **TA-Lib (optional)** — C-backed technical-analysis library with Python bindings. · **Role in our stack:** optional reference/speed path where a specific C-backed formula is worth the native dependency. · **Key features:** ~150 functions, fast C implementations, well-established reference definitions, abstract API for batch indicator calls.
- **Owned walk-forward harness (committed)** — explicit forecast/backtest evaluation code owned by this product. · **Role in our stack:** the committed evaluation engine (§4) — forecast-first, point-in-time, purged walk-forward evaluation with proper scoring rules, coverage, and cost-aware signal checks. · **Key features:** auditable logic, no licensing gray zone, metrics aligned with the calibration scoreboard.
- **backtrader (not adopted)** — event-driven backtesting and trading framework. · **Role in our stack:** *not part of the committed stack* — §4 commits an owned walk-forward evaluation harness. Kept as a reference for event-driven, order-level fill simulation should the owned harness prove insufficient. · **Key features:** event-driven engine, broker/commission/slippage simulation, built-in indicators and analyzers, live-trading broker hooks.

#### Containerization, CI/CD & Observability
- **Docker + docker-compose** — container runtime and multi-service local orchestration. · **Role in our stack:** packages the API, workers, and ML services into reproducible images and wires up Postgres/TimescaleDB, Redis, and the app for local/dev environments. · **Key features:** multi-stage builds for slim images, `compose` service graph with networks/volumes, health checks and dependency ordering, env-driven configuration.
- **GitHub Actions** — CI/CD automation native to GitHub. · **Role in our stack:** runs lint/type-check/tests on every PR and builds/pushes images and deploys on merge. · **Key features:** matrix builds, reusable workflows and caching, OIDC-based cloud auth (no long-lived secrets), environment protection rules and manual approvals.
- **Kubernetes (optional, at scale)** — container orchestration platform. · **Role in our stack:** production scaling target when single-host Compose is outgrown — running API, worker, and serving deployments. · **Key features:** declarative deployments with rolling updates and self-healing, Horizontal Pod Autoscaling, Services/Ingress for routing, ConfigMaps/Secrets and resource limits.
- **Prometheus + Grafana** — metrics collection/alerting plus visualization. · **Role in our stack:** scrapes API/worker metrics (latency, error rate, prediction throughput, queue depth) and dashboards them with alerting. · **Key features:** pull-based scraping and time-series storage, PromQL queries, Alertmanager routing, Grafana dashboards and threshold alerts.
- **Sentry** — application error-monitoring and performance platform. · **Role in our stack:** captures unhandled exceptions and traces across the FastAPI app and pipelines for fast incident triage. · **Key features:** rich stack traces with breadcrumbs, release tracking and regression detection, performance tracing/spans, alerting and source-context grouping.
- **structlog / structured logging** — structured, context-aware logging for Python. · **Role in our stack:** emits JSON logs with request IDs and correlation context for machine-parseable, queryable observability. · **Key features:** processor pipelines, bound context (per-request/per-task fields), JSON renderer for log aggregation, stdlib-logging interoperability.

---

## 6. Phased Roadmap & Delivery Plan

P0 — Foundations
- **Goal:** Stand up the skeleton, contracts, and guardrails so every later phase ships against a stable spine.
- **Deliverables / features:**
  - Repo scaffold, dependency management, formatting/linting, pre-commit hooks.
  - Web framework + ASGI server (FastAPI/Uvicorn or equivalent), config via env vars, layered settings (dev/stage/prod).
  - OpenAPI spec auto-generated; versioned routing under `/v1`.
  - Core cross-cutting concerns: structured logging, request IDs, error envelope, pagination convention, consistent error codes.
  - Auth scaffolding (API keys), per-key rate limiting middleware (stub limits), CORS.
  - `/healthz` and `/readyz` endpoints; basic metrics endpoint.
  - Local dev via Docker Compose (app + Postgres + Redis); CI pipeline (lint, type-check, unit tests).
- **Exit criteria:** `docker compose up` serves `/healthz` returning 200; OpenAPI docs render; CI green on an empty-but-wired endpoint; a request flows end-to-end with a logged request ID and auth check.

P1 — Data Ingestion
- **Goal:** Reliably pull, normalize, and store the market data everything else depends on.
- **Deliverables / features:**
  - Provider adapters behind a common interface (e.g. one EOD/intraday price source + one fundamentals source + one news source), with API-key/secret management.
  - Canonical schemas: symbols/securities master, OHLCV bars (daily + intraday), corporate actions (splits/dividends), fundamentals statements, news items.
  - Ingestion jobs (scheduled + on-demand backfill) with idempotent upserts, dedup, and watermark/cursor tracking.
  - Adjusted-price computation (split/dividend adjustment).
  - Data-quality checks (gaps, stale data, outliers) and a freshness SLA per dataset.
  - Caching layer (Redis) for hot reads; storage partitioning/indexing for time-series queries.
- **Exit criteria:** A defined universe (e.g. S&P 500 + a watchlist) backfilled with N years of daily OHLCV + latest fundamentals + recent news; nightly refresh runs green; data-quality dashboard shows gaps/freshness; adjusted vs. raw prices reconcile against a known reference.

P2 — Analysis Endpoints
- **Goal:** Expose clean, queryable read APIs over the ingested data — the product's "honest" surface before any prediction.
- **Deliverables / features:**
  - `/v1/prices` (raw + adjusted OHLCV, range/interval params), `/v1/fundamentals`, `/v1/news`.
  - `/v1/indicators`: technical indicators (SMA/EMA, RSI, MACD, Bollinger, ATR, returns/volatility) computed on demand or precomputed.
  - Symbol search/metadata endpoint; consistent date handling, timezones, and corporate-action awareness.
  - Response caching + ETags; field selection; sane defaults and limits.
  - Contract tests + golden-value tests for indicator math.
- **Exit criteria:** All read endpoints documented in OpenAPI, covered by tests, returning correct values vs. reference calculations; p95 latency within target on cached and cold paths; pagination and rate limits enforced.

P3 — Baseline Forecasting
- **Goal:** Ship an honest, well-calibrated baseline forecast so later ML has a bar to beat and the API contract for predictions is locked early.
- **Deliverables / features:**
  - `/v1/forecast` with a stable response contract: point estimate **plus prediction intervals**, horizon param, and an explicit model/version field.
  - Baseline models: naive/random-walk, drift, seasonal-naive, and a classical statistical model (e.g. ARIMA/ETS) for price or returns.
  - Forecasting service abstraction (feature window in -> forecast out) reusable by ML phase.
  - Prominent disclaimers / "not investment advice" metadata in every forecast response.
  - Evaluation harness with standard metrics (MAE, RMSE, MAPE, directional accuracy, pinball loss for intervals).
- **Exit criteria:** `/v1/forecast` returns calibrated intervals for the universe; baseline metrics logged and reproducible; a documented leaderboard exists with baselines recorded; interval coverage is empirically validated (e.g. ~80% intervals contain ~80% of outcomes).

P4 — ML Models & Backtesting
- **Goal:** Beat the baselines with proper ML and prove it with leakage-free, point-in-time backtesting.
- **Deliverables / features:**
  - Feature store / pipeline with **point-in-time correctness** (no lookahead; as-of joins for fundamentals & news).
  - ML models (e.g. gradient-boosted trees on engineered features; optionally sequence models like LSTM/Temporal Fusion) trained per-horizon, with quantile outputs for intervals.
  - Walk-forward / rolling-origin backtesting engine; transaction-cost and slippage assumptions for signal evaluation.
  - `/v1/backtest`: run/evaluate a strategy or model over a window; returns equity curve, metrics (Sharpe, max drawdown, hit rate), and per-period detail.
  - `/v1/signals`: derived buy/hold/sell or ranked-score signals with confidence, traceable to model + features.
  - Model registry + versioning; experiment tracking; champion/challenger comparison; reproducible training runs.
- **Exit criteria:** At least one ML model beats baselines on out-of-sample walk-forward metrics; backtests are reproducible and demonstrably leakage-free (point-in-time audit passes); `/v1/backtest` and `/v1/signals` documented, tested, and versioned; promoted "champion" model recorded in the registry.

P5 — Productionization
- **Goal:** Make it dependable, observable, secure, and operable under real load.
- **Deliverables / features:**
  - Model serving: low-latency inference, batch precompute for popular symbols/horizons, graceful fallback to baseline on model failure.
  - Observability: metrics, tracing, dashboards, alerting (data freshness, error rate, latency SLOs, prediction drift).
  - Reliability: retries/circuit breakers on providers, autoscaling, load/soak tests, defined SLOs and runbooks.
  - Security & compliance hardening: secret management, authz scopes/tiers, audit logging, dependency/vuln scanning, rate-limit/quota enforcement per tier, and explicit financial disclaimers/ToS surfaced via API.
  - Data/model monitoring: drift detection, scheduled retraining + automated re-evaluation gates before promotion.
  - Deployment: blue-green/canary releases, DB migrations, backups, infra-as-code, staging environment.
- **Exit criteria:** SLOs met under load test; alerting fires on injected failures; canary + rollback rehearsed; secrets externalized and scanned clean; automated retraining + promotion gate runs end-to-end; on-call runbook complete.

P6 — Scale / Expansion
- **Goal:** Broaden coverage, deepen capability, and grow the product surface once the core is trustworthy.
- **Deliverables / features:**
  - Asset/universe expansion: more equities, ETFs, FX, crypto, and additional exchanges/regions; intraday and multi-horizon forecasts.
  - Alternative data: sentiment/NLP on news & filings, macro indicators, options-implied signals.
  - Portfolio-level features: multi-asset optimization, risk analytics (VaR, factor exposures), watchlists, alerts/webhooks, and streaming (WebSocket/SSE) price + signal feeds.
  - Developer ecosystem: client SDKs, usage analytics, billing/quotas, sandbox keys, and richer API docs/examples.
  - Performance/scale: data tiering (hot/cold), query acceleration, multi-region read replicas, cost optimization.
- **Exit criteria:** New asset classes live with the same data-quality and forecast-calibration bars met; streaming + alerts in production; SDKs published; billing/quota tiers enforced; scale targets (symbols, RPS, regions) demonstrated under load.

---

### Feature scope by surface

API endpoints we'll expose (sequenced by phase):

- `GET /healthz`, `GET /readyz` — liveness/readiness probes *(P0)*
- `GET /v1/symbols` — symbol search & security metadata *(P1/P2)*
- `GET /v1/prices` — raw & adjusted OHLCV (range/interval params) *(P2)*
- `GET /v1/fundamentals` — financial statements & ratios, as-of aware *(P2)*
- `GET /v1/indicators` — technical indicators (SMA/EMA, RSI, MACD, Bollinger, ATR, volatility) *(P2)*
- `GET /v1/news` — news items & metadata per symbol *(P2)*
- `GET /v1/corporate-actions` — splits & dividends *(P2)*
- `POST /v1/forecast` (and `GET /v1/forecast`) — price/return forecast with prediction intervals, horizon & model version *(P3, ML in P4)*
- `POST /v1/backtest` — strategy/model backtest -> equity curve + risk metrics *(P4)*
- `GET /v1/signals` — buy/hold/sell or ranked scores with confidence *(P4)*
- `GET /v1/models` — model registry: versions, metrics, champion/challenger *(P4/P5)*
- `GET /metrics` — operational metrics (Prometheus) *(P0/P5)*
- `WS /v1/stream` (+ `POST /v1/alerts`, webhooks) — streaming prices/signals & alerts *(P6)*

---

### 6.1 Candidate enhancements — 2026-07-06 analysis & brainstorm (backlog, not yet committed)

A mid-2026 competitive scan (2026-07-06; re-verify periodically) **did not find a direct developer API** that combines a REST/WebSocket product with calibrated probabilistic forecasts and a live, independently verifiable calibration record. The incumbents scanned lean on self-reported backtests instead — and several are adjacent rather than direct comparators (Nixtla TimeGPT is bring-your-own-data forecasting infrastructure; Numerai Signals *buys* signals rather than selling a forecast API; Danelfin/FinBrain/Kavout are the nearest products but ship scores/intervals without published coverage reports). The product's differentiating wedge is therefore **"honest, verifiable uncertainty."** The items below are sequenced candidates to *promote* into the committed P0–P6 deliverables above; they are not themselves commitments. Effort: **S** = days · **M** = 1–2 wk · **L** = ~1 mo · **XL** = multi-month.

**Cross-cutting doctrine gates (decide before the phase they block):**
- **Model-weight licensing:** confirm Chronos-2 (and any foundation-model) weights permit commercial serving *and* fine-tuning before building on them — the not-to-do list forbids research-only weights in a paid product. *Gate before P4.*
- **Vendor Enterprise / ML-training licenses:** assign an owner + timeline to secure redistribution/ML-training rights from Polygon/FMP/Finnhub before any customer-facing launch. *Gate before P3 launch.*
- **Monetization design:** pricing tiers and quota semantics constrain the P0 rate-limit middleware and the API-key model — decide the tiering shape early even if billing itself ships in P6.

**P0 — Foundations**
- `Idempotency-Key` on `POST /v1/forecast` and `/v1/backtest` (doctrine-mandated, currently unscheduled) — *S*. *(contract header already stubbed 2026-07-06.)*
- Decide the API-key CRUD + self-serve key issuance + `GET /v1/usage` schema now to avoid rework — *M*.

**P1 — Ingestion**
- Vendor cost-guard middleware (per-vendor Redis token buckets; 80% → cache-only + alert, 100% → serve last-known-good with honest freshness headers) — *S*.
- Synthetic canary symbols (closed-form truth flowing through ingest → adjust → features → forecast, filtered at the API boundary) — *S*.
- Chaos/gap replay harness (record vendor cassettes now while cheap; replay mutated ones — missing days, late corporate actions, 429 storms) — *M*.
- Snapshot-pinning schema (`as_of`, `snapshot_id`) must land here so forecasts can reference an immutable snapshot — *L*.
- `/v1/data-quality/{symbol}` cross-vendor consensus + adjustment-factor ledger (expose mandatory hygiene as product) — *M*.
- Cross-sectional rank features via Timescale continuous aggregates (licensing-safe edge single-series models structurally lack) — *M*.

**P2 — Analysis**
- `GET /v1/universe?as_of=` survivorship-bias-free membership + `/changes` diff (Sharadar). *Gated* on written Nasdaq Data Link redistribution confirmation; the tradable/delisting half is safe, index-membership is at-risk — *M*.

**P3 — Baseline forecasting**
- Conformal calibration layer (CQR + Adaptive Conformal Inference) wrapping *every* model incl. the baselines — *M*. **← highest-value: manufactures the calibration wedge — a distribution-free coverage *target* under documented calibration assumptions (exchangeability is fragile on financial series across regime shifts, so pair with regime-conditional/adaptive variants and empirical validation).**
- Forecast provenance block in the `/v1/forecast` contract — **DONE** (contract locked + hardened 2026-07-06).
- Tamper-evident forecast hash archive from day one (SHA-256 every forecast + daily Merkle root + full symbol/model/horizon manifest, before outcomes are known) — *S*.
- Historical forecast+outcome archive as a sellable research dataset — *S*.

**P4 — ML & backtesting**
- Regime stack: `/v1/regimes` (HMM/BOCPD) + regime-conditional (Mondrian) conformal + machine-readable `interval_drivers` (why an interval is wide) — *L*. *(watch rare-regime finite-sample coverage.)*
- Model arbitration: `model=auto` FFORMA-style router + Vincentized quantile ensemble, with naive kept in the pool so unbeatable symbols get the honest naive forecast — *M*.
- Event-calendar features (days-to-earnings, FOMC/CPI proximity) as known-future covariates and interval drivers — *S*.
- Quantized Chronos-2 nightly batch precompute with baseline fallback + CPU fencing (makes foundation-model forecasts viable on one VPS) — *M*.

**P5 — Productionization**
- Public calibration scoreboard: `/v1/calibration` + model-health page ("Proof, not promises"), backed by the day-one hash archive — *M*. **← the marketing wedge; uncopyable without 12 months of honest history. Design display rules (min window, confidence bands) up front.**
- Live-ops credibility loop: versioned MLflow promotion gates + shadow champion/challenger + two-speed drift recalibration (nightly ACI + drift-triggered retrain). Note: ACI recalibration must version into the ETag/model identity or the "identical bytes" determinism promise breaks — *L*.

**P6 — Distribution & expansion**
- Official MCP server + auto-generated typed SDKs (MCP is table stakes for 2026 finance APIs; nobody serves *calibrated* forecasts over it) — *M*.
- Snapshot-pinned free sandbox (25 symbols, deterministic) as zero-friction top-of-funnel — *M*.
- Alpaca paper-trading forward-validation ledger `/v1/track-record` (strict model-validation framing, in-payload disclaimers, legal review of page copy) — *M*.
- Backtest-as-a-service (`POST /v1/backtest`, constrained strategy DSL, doctrine-enforced costs/embargo/survivorship defaults) — *L*. *(singular `/v1/backtest` to match the committed API list; standardize on RESTful job-collection semantics only if that endpoint is ever restructured.)*
- Fintech B2B layer: portfolio distribution aggregation (correlation-aware) + white-label fan-chart embed with non-removable disclaimer — *L*.
- Chronos-2 LoRA fine-tuning with a zero-shot-vs-tuned promotion gate (spot GPU, never idle) — *XL*.
- BIST / Borsa Istanbul as a defensible wedge market (native-language moat; start vendor licensing due-diligence early) — *XL*.

---

## 7. Appendix: Source Selection Summary

**Committed primary stack:** Polygon/Massive (prices) · FMP (fundamentals) · Finnhub (news/sentiment) · Sharadar (point-in-time backtest fundamentals) · Chronos-2 + Nixtla + Darts (modeling) · FastAPI + TimescaleDB + Redis (serving) · Hostinger VPS → cloud (deployment).

**Redistribution-safe upgrade path:** Databento US Equities Mini.

**Expansion:** Alpaca (paper trading validation) · Binance/CoinGecko (crypto) · OANDA (FX).

**Hard exclusions:** IEX Cloud (shut down Aug 2024) · unofficial Yahoo Finance endpoints · any vendor whose ToS forbids ML training or derived-data redistribution without a license.

---

_Generated as a planning artifact. Verify all vendor pricing/limits at the official source before committing budget — they change frequently._
