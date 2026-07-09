"""Claude CLI adapter — the executor (and plan-gate reviewer).

Invokes ``claude -p --output-format json`` with the prompt as a trailing
positional argument (never over stdin — under some shells a piped multi-line
prompt with embedded quotes never reaches the model, a bug RGE hit). The JSON
envelope is parsed for the result text; decision markers are then tail-anchored
out of that text.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from ai_dispatcher.agents.base import AgentResult, extract_markers
from ai_dispatcher.config import DispatcherConfig
from ai_dispatcher.subprocess_utils import Runner, run_command


@dataclass(frozen=True)
class ClaudeExecutor:
    """Runs Claude in headless print mode for execution and gate review."""

    config: DispatcherConfig
    runner: Runner = run_command

    def _base_argv(self, permission_mode: str, prompt: str) -> list[str]:
        argv = [*self.config.claude, "-p", "--output-format", "json"]
        if self.config.claude_model:
            argv += ["--model", self.config.claude_model]
        argv += ["--permission-mode", permission_mode, prompt]
        return argv

    def _invoke(
        self, argv: Sequence[str], markers: Iterable[str], *, timeout_s: float | None = None
    ) -> AgentResult:
        outcome = self.runner(
            list(argv),
            timeout_s=timeout_s if timeout_s is not None else self.config.model_timeout_s,
            cwd=self.config.repo_root,
            stall_threshold_s=self.config.stall_threshold_s,
        )
        if not outcome.ok:
            reason = (
                "timeout" if outcome.timed_out else "stall" if outcome.stalled else "nonzero exit"
            )
            return AgentResult(ok=False, text="", raw=outcome.stdout, error=f"claude {reason}")
        try:
            envelope = json.loads(outcome.stdout)
        except json.JSONDecodeError as exc:
            return AgentResult(
                ok=False, text="", raw=outcome.stdout, error=f"bad JSON envelope: {exc}"
            )
        if not isinstance(envelope, dict):
            return AgentResult(
                ok=False, text="", raw=outcome.stdout, error="envelope is not an object"
            )
        if envelope.get("is_error"):
            return AgentResult(
                ok=False, text="", raw=outcome.stdout, error="claude reported is_error"
            )
        text = str(envelope.get("result", ""))
        return AgentResult(
            ok=True, text=text, markers=extract_markers(text, markers), raw=outcome.stdout
        )

    def readiness_probe(self) -> bool:
        """Fail fast on broken auth before scaffolding a run."""
        argv = [*self.config.claude, "-p", "--output-format", "json", "Return exactly: ready"]
        result = self._invoke(argv, markers=(), timeout_s=180)
        return result.ok and "ready" in result.text.lower()

    def execute(self, prompt: str) -> AgentResult:
        """Perform the change described by the active packet."""
        argv = self._base_argv("acceptEdits", prompt)
        return self._invoke(argv, markers=("EXEC_STATUS", "EXEC_PACKET"))

    def plan_gate(self, prompt: str) -> AgentResult:
        """Read-only preflight review of the planned task."""
        argv = self._base_argv("plan", prompt)
        return self._invoke(argv, markers=("GATE_VERDICT",))
