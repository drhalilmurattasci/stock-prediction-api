"""Codex agent adapter tests."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ai_dispatcher.agents.codex_agent import CodexAgent
from ai_dispatcher.config import default_config
from ai_dispatcher.subprocess_utils import CommandResult


def test_plan_runs_read_only_and_reads_last_message(tmp_path: Path) -> None:
    seen: list[tuple[str, ...]] = []

    def runner(argv: Sequence[str], **_: Any) -> CommandResult:
        argv_t = tuple(argv)
        seen.append(argv_t)
        out_path = Path(argv_t[argv_t.index("--output-last-message") + 1])
        out_path.write_text("# TASK\n", encoding="utf-8")
        return CommandResult(argv=argv_t, exit_code=0, stdout="transcript", stderr="", duration_s=0)

    agent = CodexAgent(default_config(tmp_path), runner=runner)
    result = agent.plan("draft a packet")

    assert result.ok
    assert result.text == "# TASK\n"
    assert "--sandbox" in seen[0]
    assert seen[0][seen[0].index("--sandbox") + 1] == "read-only"
