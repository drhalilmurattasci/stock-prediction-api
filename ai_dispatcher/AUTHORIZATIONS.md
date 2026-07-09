# ai_dispatcher — auto-merge authorizations

Authorizations live in **`ai_dispatcher/authorizations.json`** (strict JSON), not
in this file. This document only explains the format.

An auto-merge to `main` happens **only** when a recorded, non-expired,
non-exhausted authorization covers **every** changed file. Anything else
downgrades to a PR for human review — the downgrade can never be promoted
upward. On a fresh install the store is empty (`{"authorizations": []}`), so
every `--publish main` run downgrades to a PR until you record one.

## Why JSON, not markdown

An earlier free-form markdown ledger repeatedly failed **open** across audits — a
commented-out example parsed as live, embedded/bookended HTML comments
resurrected disabled blocks, and duplicate/missing fields silently broadened
scope or removed expiry. Strict JSON removes that entire attack surface: a real
parser, mandatory strictly-typed fields, no comments, no prose, no duplicate
keys. **Every deviation fails closed (zero authorizations).**

## Format

```json
{
  "authorizations": [
    {
      "id": "docs-q3",
      "scope": ["docs/**", "*.md"],
      "max_merges": 20,
      "expires": "2026-08-31",
      "granted_by": "drhalilmurattasci"
    }
  ]
}
```

Every field is **mandatory** and strictly validated. A missing/extra field,
wrong type, duplicate key, bad date, or malformed entry invalidates the whole
store — fail closed to zero authorizations:

- `id` — short token matching `[A-Za-z0-9][A-Za-z0-9_.-]*`; becomes part of the
  audit trail.
- `scope` — non-empty list of repo-relative globs. A change auto-merges only if
  **every** changed file matches one of these. Glob items may not contain
  spaces, commas, backticks, or quotes.
- `max_merges` — positive integer; hard cap on auto-merges charged to this
  authorization.
- `expires` — **mandatory** ISO date (`YYYY-MM-DD`); after it, the authorization
  is inert. There is no "never expires" — omitting or mistyping the key drops the
  entry.
- `granted_by` — the human who recorded it (non-empty string).

## Deactivating an authorization

**Delete its entry** or let `expires` pass. There is no comment-out mechanism —
that ambiguity is exactly what kept failing open.

Note: `ai_dispatcher/**` and `.github/**` never auto-merge even with a covering
authorization (they always downgrade to a PR), so the dispatcher can never
auto-merge changes to its own safety logic or CI.
