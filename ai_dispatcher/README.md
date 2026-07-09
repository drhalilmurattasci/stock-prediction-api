# ai_dispatcher

An autonomous AI dispatch loop for the stock-prediction API — a Python port of
the RGE AI-dispatch automation (`A:\rcad\rge`), adapted to this repo's toolchain
(**uv + ruff + mypy + pytest**) and hardened against the findings of RGE's own
2026-07-08 automation audit.

## Topology — Claude executes, Codex controls

```
task (armed in dispatch.tasks.md)
   │
   ▼
[plan]     Codex drafts TASK markdown (read-only); dispatcher writes the packet
   │
   ▼
[gate]     Claude reviews the plan read-only ──► approve / needs_changes / block
   │                                              (bounded: max_plan_revisions)
   ▼  (TASK finalized: .meta.json sidecar written)
[execute]  Claude makes the change (edits only the packet's declared scope)
   │
   ▼
[scope]    fail-closed: any edit outside the declared globs aborts the dispatch
   │
   ▼
[verify]   CI-parity gate: ruff check · ruff format --check · mypy · pytest
   │                        (step-count enforced — a trimmed gate can't certify)
   ▼
[control]  Codex reviews the diff read-only ──► pass / needs_changes / block
   │                                             (bounded: max_correction_rounds)
   ▼
DispatchResult (passed | blocked | failed) — the loop NEVER commits
   │
   ▼
[publish]  separate, authorization-gated: merge to main / PR / branch
```

The control review is done by a **different model than the executor** on
purpose: the audit found same-model review ("Codex grading Codex") to be an
echo chamber. Cross-model independence is the property worth keeping.

## What the audit changed in this port

RGE's audit concluded the only genuinely load-bearing protections were the
deterministic verify gate and the fail-closed scope guard — the LLM "guard" and
same-model control were largely theater. This port therefore:

- **keeps and strengthens** the verify gate (mirrors `ci.yml` one-for-one) with
  **step-count enforcement** (`verify.is_publishable`) so a shortened or trimmed
  gate cannot self-certify — the exact hole the audit flagged;
- **keeps** the fail-closed scope guard;
- makes the one surviving LLM review **independent** (Codex controls Claude's
  work) and **diff-based**;
- **never silently escalates** the control sandbox — a read-only review that
  can't run read-only fails loudly (RGE silently escalated to full access on 41
  dispatches, voiding the guarantee that authorizes auto-publish);
- keeps publishing **fail-closed**: auto-merge to `main` requires a recorded
  authorization covering *every* changed file, else it downgrades to a PR;
- keeps bookkeeping **lean** (the audit's 3.5× paperwork-to-product ratio is the
  anti-pattern to avoid).

## Layout

```
ai_dispatcher/
  config.py          DispatcherConfig (paths, bounds, timeouts, verify steps, risk globs)
  subprocess_utils.py bounded runs: timeout + stall + cross-platform tree-kill
  agents/
    base.py          AgentResult + tail-anchored marker extraction
    claude_agent.py  executor + plan-gate (claude -p --output-format json)
    codex_agent.py   read-only planner + controller (codex exec; no escalation)
  packets.py         handoff packets (TASK/EXEC/CORRECT) + .meta.json sidecars
  scope_guard.py     git-status snapshot + glob-based out-of-scope detection
  verify.py          CI-parity gate, step-count enforced
  tasks.py           brief parsing + selection + size guard
  sentinels.py       kill switch, halt, consecutive-failure breaker, seatbelt
  loop.py            the state machine (never commits)
  publish.py         risk tiers, authorization ledger, merge/PR actions
  cli.py             python -m ai_dispatcher <verify|validate-packet|select|loop>
  schemas/           codex_control.schema.json
  dispatch.tasks.md  the armed task brief
  AUTHORIZATIONS.md  append-only auto-merge authorization ledger
  handoffs/          run-local handoff packets (gitignored; created on first run)
```

Per-run scratch (`.ai_dispatch/`) and the handoff packets (`ai_dispatcher/handoffs/`)
are gitignored — they are audit artifacts, kept out of the repo (and out of the
publish change-set) to avoid the bookkeeping bloat RGE's audit flagged.

## Usage

```bash
# run the verify gate (same checks as CI); exit 0 = green
python -m ai_dispatcher verify

# validate a handoff packet's structure
python -m ai_dispatcher validate-packet ai_dispatcher/handoffs/<id>_TASK_*.md

# show the next armed task
python -m ai_dispatcher select

# run one bounded dispatch for a task id (loop only, no publish)
python -m ai_dispatcher loop <task-id>

# run + publish a passed dispatch (pr is safest; main needs an authorization)
python -m ai_dispatcher loop <task-id> --publish pr
python -m ai_dispatcher loop <task-id> --publish main
```

`--publish main` auto-merges only if `AUTHORIZATIONS.md` has a live entry
covering every changed file; otherwise it downgrades to a PR.

## Requirements

`git`, `gh`, `claude`, and `codex` on `PATH`, `claude`/`codex` authenticated,
and the repo synced with `origin/main` on a clean tree before a run.

## Status / not-yet-verified

v1 delivers the inner loop, verify gate, scope guard, packet protocol, publish
layer, and CLI, all unit-tested via dependency injection. **Not yet exercised
end-to-end on this host:** live `codex`/`claude` model calls and the live
`git`/`gh` publish actions. Deferred to v2: the unattended scheduler + queue
driver (task auto-selection, isolated worktrees, one-retry-with-feedback,
seatbelt loop) and a health/trends readout.
