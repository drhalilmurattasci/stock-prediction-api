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
