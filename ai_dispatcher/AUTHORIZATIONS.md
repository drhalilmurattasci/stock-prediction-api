# ai_dispatcher — auto-merge authorization ledger

Append-only. Each `## AUTH <id>` block authorizes the dispatcher to auto-merge
verified changes to `main` **only** when every changed file falls within the
declared `SCOPE`, up to `MAX_MERGES` merges, until `EXPIRES`. A change not fully
covered by a live authorization is downgraded to a PR for human review — the
downgrade can never be promoted upward.

There are **no active authorizations** by default: on a fresh install every
`--publish main` run downgrades to a PR until you record one here.

Fields per block:

- `SCOPE` — comma/space-separated repo-relative globs (backticks optional). A
  change auto-merges only if EVERY changed file matches one of these.
- `MAX_MERGES` — hard cap on auto-merges charged to this authorization.
- `EXPIRES` — ISO date (`YYYY-MM-DD`); after it, the authorization is inert.
- `GRANTED_BY` — the human who recorded it.

<!-- Example (commented out — copy, uncomment, and edit to activate):

## AUTH 2026-07-docs-tests

- SCOPE: `docs/**`, `*.md`, `tests/**`
- MAX_MERGES: 20
- EXPIRES: 2026-08-31
- GRANTED_BY: drhalilmurattasci

-->
