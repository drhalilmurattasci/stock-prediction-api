# Operator scripts

Operational scripts. `db-init/*` runs on first TimescaleDB boot (see docker-compose.yml).

No real vendor-to-forecast proof is recorded yet; the ordinary database gate
uses labelled synthetic evidence and cleans it before returning. Every command
below is documentation, not authorization; date scope and call budgets must
come from a fresh owner grant where required.

`ci-live-database-gate.sh` is the GitHub-hosted-runner counterpart to the local
Windows `run-live-gate.ps1`. It refuses a checked-out `.env`, vendor credentials,
enabled automation, or reusable Postgres storage; creates a fresh loopback-only,
digest-pinned TimescaleDB container with masked ephemeral credentials; runs the
repository extension and role bootstrap; executes only the destructive Postgres
module; and removes the anonymous database volume afterward. It cannot run a
vendor lane and is not an operator authorization mechanism.

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

All three modes require a clean `main` worktree; the commit is part of the plan.
Smoke, typed acquisition, the lower-level backfill, ordinary close ingestion,
and ordinary Polygon price ingestion
share one PostgreSQL vendor-operation lock, so no controlled Polygon lane can
overlap an authorized run. See `INSTALL.md` for the owner authorization and
failure/recovery runbook.

`vendor_acquisition.py` and `run-vendor-acquisition.ps1` are the preferred
typed action-plus-price lane for the adjusted-data milestone. They compose the
proven price coverage/checkpoint machinery with immutable split and dividend
collection publication under one content-addressed plan, one advisory lock, one
5-calls-per-60-seconds pacer, and one cumulative campaign budget:

- `plan` is read-only, needs no `POLYGON_API_KEY`, accepts only `-End`, and
  reports the exact `plan_id`, stable `campaign_id`, campaign-ledger digest,
  campaign and global ledger counts/digests anchored in Postgres, cumulative
  reservations/authorized calls, exact required budget delta,
  call-set digest, typed allocation, receipt-only repairs, prior-attempt
  ambiguity, and whether the one-bar smoke anchor exists.
- `repair` accepts only that current plan ID. It may publish missing database
  receipts for already committed price/action content but admits zero outbound
  requests.
- `execute` requires the current plan and campaign identities, the plan's exact
  campaign budget delta, exact current-run and per-kind ceilings, the
  `stockapi-msft-acquisition-only` sentinel, and a fresh lowercase authorization
  ID. The sentinel is only a mechanical check; it never replaces an owner grant
  naming the reviewed plan, cumulative state, delta, and allocation.

From a zero-data database, `plan` is correctly `blocked` until the separately
authorized one-request smoke has established the latest-session receipt. After
that smoke, with no action collections or other bars present, the expected plan
is exactly **259 calls**: `split_page=1`, `dividend_page=1`, and
`open_close=257`. Execution must pass those exact values:

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

The ordered call set sends the complete split page first, then the complete
dividend page, then unique missing open-close sessions. Each corporate-action
request is a single page and rejects `next_url` rather than following it. Each
attempt is reserved before HTTP in ignored, append-only
`data/vendor_acquisition_attempts.jsonl`; HTTP retries are disabled, and the
exact content plus its later receipt is checkpointed before the next call. The
database stores an immutable high-water row after every authorization,
reservation, and outcome. The global file count/digest must match that row even
when a later plan belongs to a different session-window campaign, so truncation
or rollback cannot manufacture fresh budget. The lane also reads the older
`data/vendor_backfill_attempts.jsonl` ambiguity state,
so preserve both ledgers. The first authorization establishes an exact campaign
base with zero automatic retry allowance. Any later plan that needs another
attempt reports an exact positive recovery delta; that delta needs a separate
owner grant and the cumulative campaign can never receive more than five
recovery calls. A consumed/ambiguous reservation has no clear switch: perform
vendor/database forensics, re-plan, and obtain a new authorization for only an
unambiguous remaining call set. The wrapper scrubs ambient vendor credentials
and proxies, revalidates the plan, and executes from a detached reviewed Git
revision while keeping the canonical ledger in the primary workspace. It does
not require or perform a push. Mutating modes additionally refuse a running API,
worker, Beat, snapshot-builder, native Celery/uvicorn, or another operator.

Factor calculation/publication is deliberately separate from acquisition.
`AdjustmentFactorBuilder` selects exact cutoff-visible raw versions and action
collections, the pure policy computes canonical Decimal34 factors, and
`SqlAdjustmentFactorSetStore` publishes immutable content followed by a later
receipt. The low-level one-shot primitive is
`python -m ingestion.tasks.seal_adjusted_forecast_snapshot`. It performs no
vendor I/O and is not a Celery task; inside one revision-attested
`stockapi_snapshot_builder` image it publishes or replays one MSFT factor set at
the operator-plan-bound cutoff and then creates or replays one adjusted-close
snapshot at the later factor-receipt time.
It requires the exact `stockapi-msft-adjusted-seal-only` sentinel, current
258-session XNYS scope, and both adjusted policy hashes pinned to the running
code. Its complete interface contract, shown for review rather than direct host
execution, adds:

```text
--end YYYY-MM-DD
--factor-cutoff <aware-ISO-8601-plan-cutoff>
--expected-factor-set-id <sha256:exact-read-only-plan-identity>
--tool-revision <40-hex-reviewed-commit>
--authorization stockapi-msft-adjusted-seal-only
```

The image must contain that exact baked revision, and pre/post database-clock
checks enforce cutoff freshness, receipt visibility, and session currency. The
sentinel is only a refusal check, and acquisition authority does not authorize
this local DB write. Do not invoke the primitive ad hoc. Instead use the
`adjusted_close` lane of `run-forecast-demo.ps1`: its read-only plan requires a
complete exact acquisition, prepares the real factor artifact without publishing,
and binds the resulting factor ID. Execute requires the distinct owner-facing
`stockapi-msft-adjusted-seal-serve-only` authorization. The public adjusted-price
route requires a resulting exact factor ID; it never resolves “latest.”

`forecast_demo.py` is the raw-close local seal-and-serve lane behind
`run-forecast-demo.ps1`; `adjusted_forecast_demo.py` is the adjusted-close lane,
and the wrapper selects only those fixed modules from `-Target`. Plan mode is
read-only and requires the exact
258-session MSFT price coverage produced by the typed acquisition (or proven by
the shared lower-level planner), database-clock session currency, all exact
current-version receipts, one configured API key, the code-derived policy pins,
and a clean Git commit. Raw `execute` requires that content-addressed plan plus
the exact `stockapi-msft-seal-serve-only` sentinel. It starts only the loopback API and
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
The adjusted lane separately proves exact raw/split/dividend/factor lineage and
uses a deterministic plan-bound POST idempotency key so retry must replay one
archived run. Neither lane authorizes adjusted outcome/cohort evidence.
