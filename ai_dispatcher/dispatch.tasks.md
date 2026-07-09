# ai_dispatcher — task brief

This is the **authorized source of work** for the dispatcher. Each task is a
`## <id>: <title>` section with a `- STATUS:` line. Only `armed` tasks are
selectable; `unarmed`, `done`, and `blocked` tasks are skipped.

Keep this file well under ~900 KB — the whole brief is embedded in the
selection prompt, and an oversized brief will halt selection (archive completed
entries to `ai_dispatcher/dispatch.tasks.archive.md`).

Arm a task by setting its `- STATUS:` to `armed`. Nothing runs autonomously
until you arm it **and** (for auto-merge to `main`) record an authorization in
`ai_dispatcher/AUTHORIZATIONS.md`.

<!-- Task ids become branch names (ai-dispatch/<id>) and dispatch run dirs, so
keep them short and filesystem-safe: letters, digits, dot, dash, underscore. -->

## ingest-retry-hardening: Surface a total non-retryable ingest outage as a failure

- STATUS: unarmed

When every symbol fails with only non-retryable errors, `ingest_prices`
currently returns `status='failed'` without raising, so Celery marks the beat
task SUCCESS. Make the sync entrypoint `raise` when `status=='failed'` and
`retryable_failures==0`. Also default unknown exceptions in
`_is_retryable_symbol_error` to non-retryable and allow-list only transient
types. Add a test driving the sync `ingest_prices` entrypoint through both the
retry and raise branches with a bound-task stub. MAY edit:
`ingestion/tasks/ingest_prices.py`, `tests/unit/test_ingest_prices_task.py`.

## example-docs-task: (example) tidy a docstring

- STATUS: unarmed

Example of a low-risk, auto-merge-eligible task (touches only docs). Left
unarmed; arm it and add a matching authorization to try the full path.

## dispatch-smoke-doc: Create a dispatcher smoke-test note

- STATUS: armed

Create `docs/dispatch_smoke.md` with exactly one short line:
`Dispatcher smoke test passed.`

Do not edit any other files. MAY edit: `docs/dispatch_smoke.md`.
