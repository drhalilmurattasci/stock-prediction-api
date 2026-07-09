"""Codex CLI adapter — the planner (drafts the TASK packet) and the controller.

The controller runs read-only with structured JSON output (``--output-schema``
+ ``--output-last-message``). Crucially, unlike RGE, an ACL/sandbox failure is
**never** silently retried at a higher permission level: a read-only control
review that cannot run read-only fails loudly, because a silent escalation to
``danger-full-access`` voids the read-only guarantee that authorizes
auto-publish (RGE audit finding).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_dispatcher.agents.base import AgentResult
from ai_dispatcher.config import DispatcherConfig
from ai_dispatcher.subprocess_utils import Runner, run_command

_ACL_MARKERS = ("permission denied", "access is denied", "sandbox", "operation not permitted")


@dataclass(frozen=True)
class CodexAgent:
    """Runs Codex for planning and control.

    Both are read-only model calls. The dispatcher writes the planner's final
    TASK markdown into the scaffolded packet itself, which avoids depending on
    Codex's write sandbox and ensures the planner can never edit product files.
    """

    config: DispatcherConfig
    runner: Runner = run_command

    def _argv(self, sandbox: str, *, schema: Path | None, out_message: Path | None) -> list[str]:
        argv = [
            *self.config.codex,
            "exec",
            "--cd",
            str(self.config.repo_root),
            "--sandbox",
            sandbox,
        ]
        if self.config.codex_model:
            argv += ["--model", self.config.codex_model]
        if schema is not None:
            argv += ["--output-schema", str(schema)]
        if out_message is not None:
            argv += ["--output-last-message", str(out_message)]
        argv.append("-")
        return argv

    def plan(self, prompt: str) -> AgentResult:
        """Draft the TASK packet content as a read-only final message."""
        out_message = self.config.ai_dir / "codex-plan-last-message.md"
        out_message.parent.mkdir(parents=True, exist_ok=True)
        out_message.unlink(missing_ok=True)
        argv = self._argv("read-only", schema=self.config.plan_schema_path, out_message=out_message)
        outcome = self.runner(
            argv,
            timeout_s=self.config.model_timeout_s,
            cwd=self.config.repo_root,
            stdin=prompt,
            stall_threshold_s=self.config.stall_threshold_s,
        )
        if not outcome.ok:
            return AgentResult(
                ok=False, text=outcome.stdout, raw=outcome.stdout, error="codex plan failed"
            )
        text = out_message.read_text(encoding="utf-8") if out_message.exists() else outcome.stdout
        return AgentResult(ok=True, text=text, raw=outcome.stdout)

    def control(
        self, prompt: str, *, out_message: Path
    ) -> tuple[AgentResult, dict[str, Any] | None]:
        """Independent read-only control review returning a schema-checked verdict."""
        out_message.parent.mkdir(parents=True, exist_ok=True)
        argv = self._argv(
            "read-only", schema=self.config.control_schema_path, out_message=out_message
        )
        outcome = self.runner(
            argv,
            timeout_s=self.config.model_timeout_s,
            cwd=self.config.repo_root,
            stdin=prompt,
            stall_threshold_s=self.config.stall_threshold_s,
        )
        combined = f"{outcome.stdout}\n{outcome.stderr}".lower()
        if not outcome.ok:
            if any(marker in combined for marker in _ACL_MARKERS):
                # Do NOT escalate. A read-only review that cannot run read-only
                # is a hard failure — escalating would void the guarantee that
                # authorizes auto-publish.
                return (
                    AgentResult(
                        ok=False,
                        text="",
                        raw=outcome.stdout,
                        error="codex read-only sandbox denied",
                    ),
                    None,
                )
            return (
                AgentResult(ok=False, text="", raw=outcome.stdout, error="codex control failed"),
                None,
            )
        verdict = _load_json(out_message)
        if verdict is None:
            return (
                AgentResult(
                    ok=False,
                    text=outcome.stdout,
                    raw=outcome.stdout,
                    error="control JSON missing/invalid",
                ),
                None,
            )
        return AgentResult(ok=True, text=outcome.stdout, raw=outcome.stdout), verdict


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None
