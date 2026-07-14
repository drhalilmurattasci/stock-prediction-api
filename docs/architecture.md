# Architecture

The authoritative product roadmap remains
[STOCK_API_MASTER_PLAN.md](../STOCK_API_MASTER_PLAN.md). This page records the
implemented first-forecast trust boundary.

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
Adjusted prices require the planned versioned corporate-action
factor ledger; vendor-rewritten history is not treated as that ledger.

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

Archive persistence uses an optimistic two-phase flow: a short keyed lookup,
snapshot loading and pure forecast computation with no archive connection held,
then a short advisory-lock/recheck/insert transaction. Two simultaneous first
uses of one key may duplicate bounded computation, but the full-digest unique
constraint permits exactly one persisted result; the loser discards its local
output and replays the winner or returns `idempotency_in_progress`. Persisted
`generated_at` and leakage-check time are the archive database's observed
post-compute completion time, while `recorded_at` remains its later acceptance
stamp. This keeps the time-order invariant in one clock domain.

The remaining Phase 3 trust gaps are append-only realized-outcome and calibration
artifacts, stable idempotency identity across credential/secret rotation, a
reproducible model registry/leaderboard, and models that empirically beat the
baselines. Daily forecast manifests/external anchoring remain beyond the per-run
SHA-256 archive. Later endpoint families remain phased backlog.
