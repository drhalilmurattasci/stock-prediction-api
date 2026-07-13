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

`forecast_demo.py` is the final local seal-and-serve lane behind
`run-forecast-demo.ps1`. Its `plan` mode is read-only and requires the exact
258-session MSFT backfill, database-clock session currency, all exact current
version receipts, one configured API key, the code-derived policy pins, and a
clean Git commit. `execute` requires that content-addressed plan plus the exact
`stockapi-msft-seal-serve-only` sentinel. It starts only the loopback API and
runs one short-lived container with the `stockapi_snapshot_builder` credential;
it never starts a persistent queue consumer, ordinary worker, or Beat. The
wrapper builds from an exact detached Git worktree, binds both roles to the
same revision-labelled immutable image, starts the API with that image ID, and
runs the builder with the same ID plus `--pull never`. The controller also
requires the freshly recreated API container ID, Compose project/service
labels, and zero mounts. After a validated seal, any remaining proof failure is
reported as a sanitized `sealed_proof_failed` recovery receipt rather than
hiding the committed snapshot. The controller proves unauthenticated `401`,
authenticated pre-seal `404`, the
sealed row through the runtime role, and authenticated `200` parsed as the
locked `ForecastResponse`. This lane has no vendor-provider import and makes no
vendor call. It also proves a wrong key returns `401`, disables ambient HTTP
proxies, binds API-key identity with a nonpublic JWT-keyed HMAC inside the plan,
and compares the response's exact XNYS schedule, deterministic naïve values,
0.8 interval bucket, and source-manifest lineage to the sealed bytes.
