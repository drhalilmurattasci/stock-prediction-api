"""Fake-driven integration test of the DispatchLoop control flow.

No live codex/claude/git: planner, executor, controller, and the git/verify
runners are all fakes, so the state-machine branches are exercised
deterministically. The git-status sequences model each round starting clean —
enough to drive the flow, not a faithful git simulation.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ai_dispatcher.agents.base import AgentResult
from ai_dispatcher.config import default_config
from ai_dispatcher.loop import DispatchLoop
from ai_dispatcher.subprocess_utils import CommandResult
from ai_dispatcher.tasks import Task

_VALID_TASK = """# TASK — {tid}

- DISPATCH_ID: {tid}
- AUTHOR: planner:codex
- TIMESTAMP: 2026-07-09_10-00-00+0300
- RELATED_FILES: `{glob}`
- STATUS: ready

## Goal

do it

- HANDOFF_STATUS: COMPLETE
- DISPATCH_ID: {tid}
- NEXT_ROLE: EXECUTOR_AI
- EXIT_CODE: 0
"""


class FakePlanner:
    def __init__(self, config: Any, *, glob: str = "docs/**") -> None:
        self.config = config
        self.glob = glob

    def plan(self, _prompt: str) -> AgentResult:
        return AgentResult(ok=True, text=_VALID_TASK.format(tid="demo", glob=self.glob))


class FakeExecutor:
    def __init__(self, *, gate: str = "approve", exec_status: str = "executed") -> None:
        self.gate = gate
        self.exec_status = exec_status

    def plan_gate(self, _prompt: str) -> AgentResult:
        return AgentResult(ok=True, text="", markers={"GATE_VERDICT": self.gate})

    def execute(self, _prompt: str) -> AgentResult:
        return AgentResult(
            ok=True, text="", markers={"EXEC_STATUS": self.exec_status, "EXEC_PACKET": "x.md"}
        )


class FakeController:
    def __init__(self, verdicts: list[str]) -> None:
        self.verdicts = verdicts
        self.calls = 0

    def control(
        self, _prompt: str, *, out_message: Path
    ) -> tuple[AgentResult, dict[str, Any] | None]:
        verdict = self.verdicts[min(self.calls, len(self.verdicts) - 1)]
        self.calls += 1
        return AgentResult(ok=True, text=""), {"verdict": verdict, "required_fixes": ["fix it"]}


class FakeGit:
    def __init__(self, status_outputs: Sequence[str]) -> None:
        self._status = list(status_outputs)

    def __call__(self, argv: Sequence[str], **_: Any) -> CommandResult:
        argv_t = tuple(argv)
        if "rev-list" in argv_t:
            return CommandResult(
                argv=argv_t, exit_code=0, stdout="0\t0\n", stderr="", duration_s=0.0
            )
        if "status" in argv_t:
            out = self._status.pop(0) if self._status else ""
            return CommandResult(argv=argv_t, exit_code=0, stdout=out, stderr="", duration_s=0.0)
        return CommandResult(argv=argv_t, exit_code=0, stdout="", stderr="", duration_s=0.0)


class FakeVerify:
    def __init__(self, *, fail_first: int = 0) -> None:
        self.calls = 0
        self.fail_first = fail_first

    def __call__(self, argv: Sequence[str], **_: Any) -> CommandResult:
        self.calls += 1
        code = 1 if self.calls <= self.fail_first else 0
        return CommandResult(argv=tuple(argv), exit_code=code, stdout="", stderr="", duration_s=0.0)


def _loop(
    tmp_path: Path,
    *,
    executor: FakeExecutor,
    controller: FakeController,
    git: FakeGit,
    verify: FakeVerify,
    glob: str = "docs/**",
    max_rounds: int = 1,
) -> DispatchLoop:
    config = default_config(tmp_path).with_overrides(max_correction_rounds=max_rounds)
    return DispatchLoop(
        config,
        planner=FakePlanner(config, glob=glob),  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        controller=controller,  # type: ignore[arg-type]
        git=git,
        verify_runner=verify,
    )


TASK = Task(task_id="demo", title="demo", body="do it", status="armed")


def test_happy_path_passes(tmp_path: Path) -> None:
    loop = _loop(
        tmp_path,
        executor=FakeExecutor(),
        controller=FakeController(["pass"]),
        git=FakeGit(["", " M docs/x.md"]),
        verify=FakeVerify(),
    )
    result = loop.run(TASK)
    assert result.status == "passed"
    assert result.changed_files == ("docs/x.md",)
    assert result.control_verdict == {"verdict": "pass", "required_fixes": ["fix it"]}


def test_scope_violation_fails(tmp_path: Path) -> None:
    loop = _loop(
        tmp_path,
        executor=FakeExecutor(),
        controller=FakeController(["pass"]),
        git=FakeGit(["", " M app/secret.py"]),
        verify=FakeVerify(),
    )
    result = loop.run(TASK)
    assert result.status == "failed"
    assert result.detail == "scope violation"
    assert result.scope_violations == ("app/secret.py",)


def test_plan_gate_block_is_terminal(tmp_path: Path) -> None:
    loop = _loop(
        tmp_path,
        executor=FakeExecutor(gate="block"),
        controller=FakeController(["pass"]),
        git=FakeGit([""]),
        verify=FakeVerify(),
    )
    result = loop.run(TASK)
    assert result.status == "blocked"


def test_verify_failure_triggers_correction_then_passes(tmp_path: Path) -> None:
    loop = _loop(
        tmp_path,
        executor=FakeExecutor(),
        controller=FakeController(["pass"]),
        git=FakeGit(["", " M docs/x.md", " M docs/x.md"]),
        verify=FakeVerify(fail_first=1),
        max_rounds=1,
    )
    result = loop.run(TASK)
    assert result.status == "passed"
    assert result.rounds == 1


def test_executor_blocked_is_terminal(tmp_path: Path) -> None:
    loop = _loop(
        tmp_path,
        executor=FakeExecutor(exec_status="blocked"),
        controller=FakeController(["pass"]),
        git=FakeGit(["", ""]),
        verify=FakeVerify(),
    )
    result = loop.run(TASK)
    assert result.status == "blocked"


def test_missing_exec_status_is_terminal(tmp_path: Path) -> None:
    loop = _loop(
        tmp_path,
        executor=FakeExecutor(exec_status=""),  # no confirmation
        controller=FakeController(["pass"]),
        git=FakeGit(["", ""]),
        verify=FakeVerify(),
    )
    result = loop.run(TASK)
    assert result.status == "blocked"


def test_trimmed_verify_gate_is_not_publishable(tmp_path: Path) -> None:
    config = default_config(tmp_path).with_overrides(
        max_correction_rounds=0,
        verify_steps=(("ruff check", ("uv", "run", "ruff", "check", ".")),),
    )
    loop = DispatchLoop(
        config,
        planner=FakePlanner(config),  # type: ignore[arg-type]
        executor=FakeExecutor(),  # type: ignore[arg-type]
        controller=FakeController(["pass"]),  # type: ignore[arg-type]
        git=FakeGit(["", " M docs/x.md"]),
        verify_runner=FakeVerify(),  # all steps pass...
    )
    result = loop.run(TASK)
    # ...but the trimmed 1-step gate is below MIN_VERIFY_STEPS, so it cannot pass.
    assert result.status == "failed"
    assert "not publishable" in result.detail


def test_control_pass_but_not_ready_does_not_pass(tmp_path: Path) -> None:
    class NotReadyController:
        def control(self, _prompt: str, *, out_message: Path) -> tuple[AgentResult, dict[str, Any]]:
            return AgentResult(ok=True, text=""), {
                "verdict": "pass",
                "commit_readiness": "not_ready",
                "required_fixes": ["needs work"],
            }

    loop = _loop(
        tmp_path,
        executor=FakeExecutor(),
        controller=NotReadyController(),  # type: ignore[arg-type]
        git=FakeGit(["", " M docs/x.md"]),
        verify=FakeVerify(),
        max_rounds=0,
    )
    result = loop.run(TASK)
    assert result.status != "passed"
