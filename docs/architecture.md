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

The serving and acting tiers are mechanically separate. Compose profile `app`
starts the API only. Persistent ingestion workers, the privileged snapshot
builder, and Beat live under profile `automation`, and every Celery task checks
the default-off `AUTOMATION_ENABLED` flag before any I/O. Beat registers no jobs
while disabled; its Polygon jobs additionally require a positive finite
per-lane, per-process call cap. Fundamentals and news remain unscheduled until
they receive owned budgets. The separately authorized smoke, backfill, and demo
commands call bounded async/operator paths directly and do not enable Celery.

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

Calibration/evaluation membership is precommitted separately. A canonical
cohort manifest identifies exact `(forecast_id, step)` members derived from
`scheduled_evaluation` archive outputs and binds selection, outcome-resolution,
and availability policies. PostgreSQL stamps the manifest transaction, then
accepts its availability receipt only in a later transaction and strictly
before the earliest target. This second-transaction receipt is the evidence
that membership was durably visible before outcomes, rather than merely stamped
inside an uncommitted transaction. PostgreSQL materializes each canonical member
into a fourth relational table and validates it against the exact archived
`scheduled_evaluation` output; callers cannot insert projection rows directly.
All four evidence tables reject update, delete, and truncate. `stockapi_app` has
`SELECT`/`INSERT` on outcomes, manifests, and availability receipts but
`SELECT` only on materialized members; the snapshot-builder role has no access.

Migration `0010` and the pure canonical validators establish this storage and
validation boundary. Its live assertions are wired into the one-command
throwaway-database gate but await the next runtime execution. No unattended
outcome resolver, cohort publisher, scoreboard, or interval recalibrator is
enabled yet; those operational actors must receive explicit policy artifacts
and automation controls before use.

The remaining Phase 3 trust gaps are the controlled outcome-resolution and
cohort-publication write paths, scored calibration artifacts, stable
idempotency identity across credential/secret rotation, a reproducible model
registry/leaderboard, and models that empirically beat the baselines. Daily
forecast manifests/external anchoring remain beyond the per-run SHA-256 archive.
Later endpoint families remain phased backlog.
