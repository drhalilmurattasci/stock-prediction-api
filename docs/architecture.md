# Architecture

The authoritative product roadmap remains
[STOCK_API_MASTER_PLAN.md](../STOCK_API_MASTER_PLAN.md). This page records the
implemented first-forecast trust boundary.

The schema head is `0014_vendor_campaign_anchor`. No real vendor-to-forecast proof is
recorded yet; ordinary live-gate verification seeds only labelled synthetic
throwaway evidence and cleans it before returning. The diagrams below describe
enforced code and persistence boundaries, not populated production evidence or
validated calibration.

```text
Massive/Polygon `/v1/open-close` raw regular-session daily bars
  -> distinct `polygon_open_close` source (never the separate `afterHours` value)
  -> stockapi_app ingestion (SELECT/INSERT/UPDATE bars)
  -> bars + append-only bars_revisions
  -> stockapi_app second-transaction INSERT of a DB-stamped
     bar_version_availability receipt
  -> stockapi_snapshot_builder (SELECT history, INSERT snapshots only)
  -> PIT version reconstruction at the scheduled 17:00 UTC cutoff
  -> XNYS completed-session/gap verification
  -> canonical content-addressed forecast_input_snapshots row
  -> stockapi_app read-only snapshot repository
  -> baseline model in an API-process worker thread
  -> /v1/forecast
```

The split/dividend path is parallel and never rewrites the raw branch:

```text
typed operator acquisition (one splits page, one dividends page, then missing closes)
  -> canonical content-addressed corporate_action_collections + immutable members
  -> later corporate_action_collection_availability receipts
  -> point-in-time factor builder selects exact action collections and exact
     current-or-revision raw bar receipts at one cutoff
  -> pure Decimal34 split/dividend policy + published binary64 factor bits
  -> canonical adjustment_factor_sets + one adjustment_factor_entries row per raw input
  -> later adjustment_factor_set_availability receipt
  -> GET /v1/prices/{symbol}/adjusted?factor_set_id=sha256:...
       validates the complete factor/raw window before range filtering or pagination
  -> revision-attested adjusted factor/snapshot primitive
  -> read-only adjusted host plan + detached one-shot seal/serve controller
  -> target-routed /v1/forecast?target=adjusted_close under separate policy pins
```

Corporate-action collections bind the exact provider query scope, complete
single-page exhaustion, canonical event versions, database acceptance time, and
a distinct post-commit receipt. Factor sets bind both exact collection receipts,
every exact raw-bar version and receipt, the pinned policy/version hashes, the
cutoff and anchor, canonical Decimal strings, and the exact IEEE-754 bits used by
consumers. All collection, factor, entry, and receipt rows reject update, delete,
and truncate. A later vendor correction creates new immutable action/version and
factor identities; it cannot rewrite an older adjusted result.

The public adjusted-price route requires the caller to name the immutable
`factor_set_id`; “latest” is not an API selection rule. The loader locates every
bound raw version across the current bar or either side of append-only revision
history, rejects missing or conflicting candidates, and calls the pure adjustment
kernel over the full factor window before it slices a page. Each response exposes
factor/policy identities, exact split and dividend collection receipts, action
version IDs, full raw coverage, and each returned row's raw version/availability
receipt. Stored evidence failure returns an error and never falls back to raw.

The same current `bars` table has a deliberately separate read-only analysis
branch to `/v1/indicators`. Version one selects at most the newest 258 exact
`polygon_open_close/raw/day/1` rows before an optional exclusive observation
timestamp, verifies that they are consecutive XNYS regular-session closes, and
computes the owned causal indicator bundle. Its response identifies the pinned
formula policy, fixed calculation-window policy, and exact ordered input rows
independently. EMA, RSI, MACD, and ATR seeds are window-relative, structural
warm-up stays null, and an `end` bound is not an availability cutoff: this is a
current-snapshot API, not point-in-time evidence. It neither certifies that the
latest completed session is present nor reconciles raw split/dividend effects.

The API never creates snapshots on demand. Snapshot rows are born sealed in one
insert; PostgreSQL stamps `sealed_at`, verifies the SHA-256, and rejects update,
delete, or truncate. On a builder retry, the builder reconstructs the
point-in-time source ledger and accepts an existing row only when its canonical
bytes still match. Serving does not replay the source ledger: it independently
reparses and canonicalizes the sealed bytes, checks their digest plus every
header and request binding, and accepts availability evidence only when its
rule-set hash matches the operator-pinned trust identity.

Policy v1 deliberately supports raw `close` and `trading_day` only. Its identity
covers the source key, fixed five-symbol US-equity/USD universe, XNYS calendar,
calendar/pandas/tzdata versions, 512-observation cap, 258-observation minimum,
252 targets, admissible database cutoffs, and the daily 17:00 UTC default. The
availability identity separately requires an exact-version post-commit receipt;
the in-transaction `recorded_at` stamp is never presented as commit visibility.
From at most 512 candidates, the builder retains the newest contiguous finalized
suffix through the latest completed session and still requires at least 258
observations; an older gap therefore cannot poison an otherwise sufficient
recent window. A change to any of those choices rotates the relevant hash.
The adjusted-close snapshot policy is independently content-addressed and
operator-pinned. It admits only `split_dividend_adjusted/day/1` observations
derived from a factor receipt visible by the snapshot cutoff. Raw-close and
adjusted-close services are routed by target and cannot borrow each other's
resolution or availability hashes. The local seal-and-serve wrapper exposes
fixed raw and adjusted targets while sharing the same host attestation and actor
exclusions. The separate low-level
`ingestion.tasks.seal_adjusted_forecast_snapshot` primitive performs no vendor
I/O and is not a Celery task: in an exact revision-attested
`stockapi_snapshot_builder` image, it publishes or replays one MSFT factor set
at an operator-plan-bound cutoff and seals or replays one adjusted-close
snapshot at the later factor-receipt time. It is bound to the exact 258-session
current XNYS window, builder-role `stockapi_test` database, two adjusted policy
pins, explicit end session, aware cutoff, and reviewed revision. Pre/post
database-clock checks enforce cutoff freshness and session currency. The fixed
sentinel is a code-level refusal check, not owner authority for the local writes.
The adjusted host plan prepares the exact artifact read-only, binds its content
ID and stable input-receipt cutoff, and keeps publication state outside the plan
identity so a partial seal can replay safely. Its final authenticated POST uses
a deterministic plan-bound idempotency key. No adjusted factor set or snapshot
has yet been published from real vendor data.

The current serving identity is equally explicit. `model=auto` executes
`baseline-naive@1`; every unkeyed invocation receives a new archived forecast UUID,
while a POST retry in the same authenticated credential/identity-secret epoch,
with the same `Idempotency-Key` and normalized request, replays the validated
canonical stored output. Credential or identity-secret rotation intentionally
starts a new namespace until stable API-principal aliases and secret-version
lookup land. The current
`feature_set_hash` is the content-addressed snapshot ID because the only features
are the sealed close series. Intervals are prediction intervals derived from baseline residuals, not
confidence intervals for an estimated parameter. Serving reports
`calibration.method=none`, no held-out coverage evidence, and an
`uncalibrated:<model-version>` calibration identity.

The serving and acting tiers are mechanically separate. Compose profile `app`
starts the API only. Persistent ingestion workers, the privileged snapshot
builder, and Beat live under profile `automation`, and every Celery task checks
the default-off `AUTOMATION_ENABLED` flag before any I/O. Beat registers no jobs
while disabled; its Polygon jobs additionally require a positive finite
per-lane, per-process call cap. Fundamentals and news remain unscheduled until
they receive owned budgets. The separately authorized smoke, typed acquisition,
lower-level backfill, and raw demo commands call bounded async/operator paths
  directly and do not enable Celery. The adjusted one-shot controller is likewise
  non-Celery and is exposed only through the fixed `adjusted_close` demo target.

Archive persistence uses an optimistic two-phase flow: a short keyed lookup,
snapshot loading and pure forecast computation with no archive connection held,
then a short advisory-lock/recheck/insert transaction. Two simultaneous first
uses of one key may duplicate bounded computation, but the full-digest unique
constraint permits exactly one persisted result; the loser discards its local
output and replays the winner or returns `idempotency_in_progress`. Persisted
`generated_at` and leakage-check time are the archive database's observed
post-compute completion time, while `recorded_at` remains its later acceptance
stamp. This keeps the time-order invariant in one clock domain.

Forecast evidence is append-only and policy-explicit rather than one mutable
notion of "truth." A realized raw-close outcome carries both the outcome
resolution-policy hash and the availability-rule-set hash, stores strict
canonical bytes under a SHA-256 identity, and binds the complete key of one
`bar_version_availability` row, including its database-stamped `available_at`.
Before accepting the row, PostgreSQL resolves that exact version across the
current bar and append-only revisions and derives/checks its close, fetched
time, and source-as-of time. The target close, copied source value, resolution
cutoff, and database-stamped seal must form one ordered timeline. A restatement
can therefore produce new evidence under an explicit policy; it cannot rewrite
an older outcome row or attach an unreceipted scalar to it.

The owned resolver makes the policy executable without making it implicit. Its
constructor requires a positive bounded resolution lag; that lag, the exact
XNYS calendar and dependency versions, raw-close source contract, USD resolver,
and version-selection rule are covered by the policy hash. The outcome table's
semantic key deliberately excludes the cutoff, so the resolver requires the
one deterministic cutoff `target_time + resolution_lag_seconds` exactly. It
takes the same transaction-scoped series lock as ingestion receipt publication,
then samples the database clock and reconstructs current, previous, and incoming
versions. Among exact receipts visible by the cutoff it accepts only one
distinct candidate at the greatest `version_recorded_at`; receipt timestamp
ordering never chooses the winner.

Persistence performs a content-ID and semantic-key preflight, submits only the
content ID and canonical evidence through a bounded `SECURITY DEFINER`
publisher, and rereads both the committed outcome and exact publication link in
a fresh session. PostgreSQL first checks exact canonical bytes, the immutable
registered policy, READ COMMITTED isolation, deterministic cutoff, USD, and the
unique newest cutoff-visible version. It then requires a pre-target-sealed
cohort member whose canonical scheduled output names the same immutable input
snapshot and target step. Exact replay may append another valid cohort-member
link; it cannot reuse an unrelated source without re-entering those database
checks. Ambiguous commit outcomes are reconciled only when both the outcome and
requested publication link are visible, or reported honestly as unknown.
Deterministic conflicts and corrupt evidence are never converted into retries.

Calibration/evaluation membership is precommitted separately. A canonical
cohort manifest identifies exact `(forecast_id, step)` members derived from
`scheduled_evaluation` archive outputs and binds selection, outcome-resolution,
and availability policies. PostgreSQL stamps the manifest transaction, then
accepts its availability receipt only in a later transaction and strictly
before the earliest target. This second-transaction receipt is the evidence
that membership was durably visible before outcomes, rather than merely stamped
inside an uncommitted transaction. PostgreSQL materializes each canonical member
into a relational table and validates it against the exact archived
`scheduled_evaluation` output; callers cannot insert projection rows directly.
The policy registry, outcomes, publication links, manifests, materialized
members, and cohort seals all reject update, delete, and truncate.
`stockapi_app` can register exact supported policy bytes and call the validated
publisher, but has no table- or column-level outcome INSERT. It retains the
minimum manifest/seal writes and read access required by the internal seam; the
snapshot-builder role has no outcome-evidence access.

Migrations `0010`-`0011`, the pure canonical validators, and the default-off scheduled
evaluation service establish this storage and publication boundary. A scheduled
spec must pin the snapshot, concrete model version, build revision, selected
steps, and all forecast/selection/outcome policy hashes. The service archives
the run, rereads and validates the committed row, derives membership only from
those persisted bytes, and publishes the manifest and seal in distinct
transactions. Exact replay, deadline refusal, and real manifest/seal races pass
the one-command throwaway-database gate on PostgreSQL 17. No Celery task, Beat
entry, default selection policy, unattended collector, scoreboard, or interval
recalibrator is enabled yet. The default-off outcome collection seam accepts
only a cohort ID, forecast ID, step, and deterministic cutoff. It freshly
validates the manifest, relational member projection, distinct seal, and
historical scheduled run under the run's stored policy epoch; rederives the
member from canonical output; checks the precommitted outcome policy identities;
then resolves and persists the exact raw-close evidence. Caller-supplied symbol,
target, currency, forecast value, or bar value never crosses that boundary.

The live gate proves the resolver with a historical XNYS close, a direct
receipt transaction held across the cutoff, fresh post-wait visibility, exact
replay, and a post-cutoff restatement. A separately labelled synthetic target
exercises successful publication and exact source-link replay in the throwaway
database, including rejection of false canonical snapshot provenance. It is not
a market observation. The database requires a real cohort seal before its XNYS
target, while real resolution must occur after that target plus the hashed lag;
that composed proof needs a forecast published in advance and elapsed market
time.

The remaining Phase 3 trust gaps are that first genuinely matured cohort,
scored calibration artifacts, stable idempotency identity across
credential/secret rotation, a reproducible model registry/leaderboard, and
models that empirically beat the baselines. Daily
forecast manifests/external anchoring remain beyond the per-run SHA-256 archive.
Later endpoint families remain phased backlog.

Versioned offline conformal kernels implement the finite-sample corrected-rank
selector, symmetric absolute-residual intervals, signed CQR corrections, and a
projected ACI state transition. They intentionally have no API, database,
scheduler, or serving adapter. In particular, ACI state can leave the canonical
thousandth grid accepted by the v1 finite-sample selector; any future composition
must pin an explicit discretization policy. A serving-grade calibration artifact
must also bind distinct prospective fit and held-out cohorts, their complete
outcome receipts, algorithm and composition identities, and post-commit
persistence evidence. Until that boundary exists and real outcomes mature,
serving remains `method=none`.

Outcome and cohort evidence is intentionally narrower than adjusted serving.
The current resolver, canonical outcome publisher, scheduled-evaluation cohort
builder, and calibration kernels accept raw close only. Adjusted forecasts must
not be enrolled into those tables or presented as scored calibration evidence
until a separately policy-hashed adjusted outcome resolver and cohort contract
exist. Migrations `0012`-`0013` establish corporate-action and factor evidence;
they do not widen the `0010`-`0011` outcome/cohort policy.
