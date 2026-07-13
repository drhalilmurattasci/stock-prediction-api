# Operator scripts

Operational scripts. `db-init/*.sql` runs on first TimescaleDB boot (see docker-compose.yml).

`vendor_smoke.py` is the deliberately narrow first-live-vendor harness behind
`run-vendor-smoke.ps1`. It accepts only MSFT, the latest completed XNYS session,
the local `stockapi_test` database, and the exact
`stockapi-vendor-smoke-only` operator sentinel. It checks that the target row is
absent, enforces a one-attempt cumulative budget, independently disables HTTP
retries, and proves the exact row plus its DB-stamped post-commit availability
receipt exist afterward. The wrapper also refuses to run alongside ordinary
worker/Beat processes and serializes concurrent wrapper invocations. The ignored
`.env` supplies the API key; never put a key on the command line.

`vendor_backfill.py` is the separate, resumable MSFT history lane behind
`run-vendor-backfill.ps1`. Its three modes are deliberately asymmetric:

- `plan` is read-only and needs no vendor key. It derives exactly the final 258
  XNYS sessions, reports current exact-version receipt coverage, and emits a
  content-addressed `plan_id`, missing-date digest, reviewed Git revision, and
  exact outbound-attempt count.
- `repair` writes only missing receipts for already committed bars and makes no
  vendor call.
- `execute` requires the plan fields, a fresh authorization ID, and the exact
  `stockapi-msft-backfill-only` sentinel. It hard-binds 5 calls per 60 seconds,
  disables HTTP retries, reserves each attempt in the ignored append-only
  `data/vendor_backfill_attempts.jsonl` ledger before sending, and checkpoints
  one bar plus its post-commit receipt before the next request.

All three modes require a clean Git worktree; the commit is part of the plan.
Smoke, backfill, ordinary close ingestion, and ordinary Polygon price ingestion
share one PostgreSQL vendor-operation lock, so no controlled Polygon lane can
overlap an authorized run. See `INSTALL.md` for the owner authorization and
failure/recovery runbook.
