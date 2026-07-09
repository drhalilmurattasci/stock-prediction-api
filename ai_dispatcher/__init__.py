"""ai_dispatcher — an autonomous AI dispatch loop for the stock-prediction API.

A Python port of the RGE AI-dispatch automation (``A:\\rcad\\rge``), adapted to
this repo's toolchain (uv + ruff + mypy + pytest) and hardened against the
findings of RGE's own 2026-07-08 automation audit.

Topology: **Claude executes, Codex controls** — the control review is performed
by a *different* model than the executor, because the audit found same-model
review ("Codex grading Codex") to be an echo chamber. The genuinely
load-bearing protections are kept and strengthened:

* the deterministic **verify gate** mirrors ``.github/workflows/ci.yml``
  one-for-one and is **step-count enforced** so a trimmed gate cannot
  self-certify;
* the fail-closed **scope guard** rejects any edit outside the task's declared
  surface;
* the loop **never commits** — publishing is a separate, authorization-gated
  step with a fail-closed PR downgrade for high-risk surfaces.

See ``ai_dispatcher/README.md`` for the full design and run modes.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
