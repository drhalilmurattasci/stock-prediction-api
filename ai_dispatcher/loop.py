"""The dispatch state machine: plan -> gate -> execute -> verify -> control.

One :meth:`DispatchLoop.run` call == one bounded task == one dispatch id. The
loop routes models and enforces the deterministic gates; it **never commits**.
Publishing is a separate, authorization-gated step (see :mod:`ai_dispatcher.publish`).

Every collaborator (planner, executor, controller, git runner, verify runner)
is injected, so the whole control flow is unit-testable with fakes — no live
``codex``/``claude``/``git`` needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from ai_dispatcher import packets
from ai_dispatcher.agents.claude_agent import ClaudeExecutor
from ai_dispatcher.agents.codex_agent import CodexAgent
from ai_dispatcher.config import DispatcherConfig
from ai_dispatcher.scope_guard import out_of_scope, parse_scope_globs, parse_status_porcelain
from ai_dispatcher.subprocess_utils import Runner, run_command
from ai_dispatcher.tasks import Task
from ai_dispatcher.verify import MIN_VERIFY_STEPS, VerifyResult, is_publishable, run_verify

DispatchStatus = Literal["passed", "blocked", "failed"]


@dataclass(frozen=True)
class DispatchResult:
    dispatch_id: str
    status: DispatchStatus
    detail: str
    changed_files: tuple[str, ...] = ()
    verify: VerifyResult | None = None
    control_verdict: dict[str, object] | None = None
    scope_violations: tuple[str, ...] = ()
    rounds: int = 0

    @property
    def publishable(self) -> bool:
        return self.status == "passed"


@dataclass
class DispatchLoop:
    """Orchestrates a single bounded dispatch."""

    config: DispatcherConfig
    planner: CodexAgent
    executor: ClaudeExecutor
    controller: CodexAgent
    git: Runner = run_command
    verify_runner: Runner = run_command
    log: list[str] = field(default_factory=list)

    # --- git helpers --------------------------------------------------------
    def _git_out(self, *args: str) -> str:
        result = self.git([*self.config.git, *args], timeout_s=120, cwd=self.config.repo_root)
        return result.stdout

    def _status_paths(self) -> set[str]:
        return parse_status_porcelain(self._git_out("status", "--porcelain=v1"))

    def _preflight(self) -> str | None:
        """Return a failure reason, or None if the base is clean and synced."""
        if self._status_paths():
            return "working tree is dirty; start from a clean base"
        counts = self._git_out(
            "rev-list", "--left-right", "--count", f"{self.config.integration_ref}...HEAD"
        )
        if counts.strip().split() not in ([], ["0", "0"]):
            return (
                f"branch not synced with {self.config.integration_ref} "
                f"(rev-list: {counts.strip()!r})"
            )
        return None

    # --- run ----------------------------------------------------------------
    def run(self, task: Task) -> DispatchResult:
        dispatch_id = task.task_id
        run_dir = self.config.run_dir(dispatch_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        preflight_error = self._preflight()
        if preflight_error is not None:
            return DispatchResult(dispatch_id, "failed", preflight_error)

        task_packet = packets.scaffold_packet(
            self.config.handoffs_dir,
            dispatch_id=dispatch_id,
            packet_type="TASK",
            author="planner:codex",
        )

        planned = self._plan_with_gate(task, task_packet)
        if planned is not None:
            return planned

        packets.finalize_packet(task_packet, packet_type="TASK")
        return self._execute_with_control(task, task_packet, run_dir)

    # --- plan + gate --------------------------------------------------------
    def _plan_with_gate(self, task: Task, task_packet: Path) -> DispatchResult | None:
        """Fill and gate the TASK packet; None means approved and finalized."""
        for revision in range(self.config.max_plan_revisions + 1):
            plan = self.planner.plan(self._plan_prompt(task, task_packet, revision))
            if not plan.ok:
                return DispatchResult(task.task_id, "failed", f"plan failed: {plan.error}")
            try:
                packet_text = packets.render_task_packet_from_plan_json(
                    plan.text, scaffold_path=task_packet
                )
            except ValueError:
                packet_text = packets.extract_packet_markdown(
                    plan.text, packet_type="TASK", dispatch_id=task.task_id
                )
            task_packet.write_text(
                packet_text,
                encoding="utf-8",
            )
            problems = packets.validate_packet(task_packet)
            if problems:
                if revision < self.config.max_plan_revisions:
                    continue
                return DispatchResult(task.task_id, "failed", f"invalid TASK packet: {problems[0]}")
            gate = self.executor.plan_gate(self._gate_prompt(task_packet))
            verdict = gate.markers.get("GATE_VERDICT", "").lower()
            self.log.append(f"plan-gate rev{revision}: {verdict or '(none)'}")
            if verdict == "approve":
                return None
            if verdict == "block":
                return DispatchResult(task.task_id, "blocked", "plan gate blocked the task")
            if verdict != "needs_changes" or revision >= self.config.max_plan_revisions:
                return DispatchResult(task.task_id, "failed", "plan gate did not approve")
        return DispatchResult(task.task_id, "failed", "plan revisions exhausted")

    # --- execute + verify + control -----------------------------------------
    def _execute_with_control(self, task: Task, task_packet: Path, run_dir: Path) -> DispatchResult:
        allowed = self._allowed_globs(task_packet)
        active_packet = task_packet
        last_verify: VerifyResult | None = None

        for rnd in range(self.config.max_correction_rounds + 1):
            execution = self.executor.execute(self._exec_prompt(active_packet, rnd))
            if not execution.ok:
                return DispatchResult(
                    task.task_id, "failed", f"executor failed: {execution.error}", rounds=rnd
                )
            exec_status = execution.markers.get("EXEC_STATUS", "").lower()
            if exec_status != "executed":
                # Missing / blocked / failed are all terminal: we never proceed
                # without an explicit execution confirmation.
                return DispatchResult(
                    task.task_id,
                    "blocked",
                    f"executor reported {exec_status or 'no status'}",
                    rounds=rnd,
                )

            # Preflight guaranteed a clean base, so the FULL current dirty set is
            # this dispatch's cumulative change (planning + every round). Scope-
            # check and publish-cover the whole set — never a per-round delta.
            changed = tuple(sorted(self._status_paths()))
            violations = tuple(out_of_scope(changed, allowed))
            if violations:
                return DispatchResult(
                    task.task_id,
                    "failed",
                    "scope violation",
                    changed_files=changed,
                    scope_violations=violations,
                    rounds=rnd,
                )

            last_verify = run_verify(self.config, runner=self.verify_runner, log_sink=self.log)
            if not last_verify.ok:
                if rnd < self.config.max_correction_rounds:
                    active_packet = self._write_correction(
                        task, run_dir, rnd, last_verify.summary_line()
                    )
                    continue
                return DispatchResult(
                    task.task_id,
                    "failed",
                    last_verify.summary_line(),
                    changed_files=changed,
                    verify=last_verify,
                    rounds=rnd,
                )
            if not is_publishable(last_verify):
                # Verify passed but the step set is below the CI-parity floor;
                # correcting cannot fix a gate-config problem, so this is terminal.
                return DispatchResult(
                    task.task_id,
                    "failed",
                    f"verify gate not publishable (steps {last_verify.steps_run}/"
                    f"{last_verify.steps_total}, min {MIN_VERIFY_STEPS})",
                    changed_files=changed,
                    verify=last_verify,
                    rounds=rnd,
                )

            control_json = run_dir / f"control.round{rnd}.json"
            control, verdict = self.controller.control(
                self._control_prompt(task_packet, changed), out_message=control_json
            )
            if not control.ok or verdict is None:
                return DispatchResult(
                    task.task_id,
                    "failed",
                    f"control failed: {control.error}",
                    changed_files=changed,
                    verify=last_verify,
                    rounds=rnd,
                )
            decision = str(verdict.get("verdict", "")).lower()
            readiness = str(verdict.get("commit_readiness", "")).lower()
            self.log.append(f"control round{rnd}: {decision or '(none)'}/{readiness or '(none)'}")
            if decision == "pass" and readiness != "not_ready":
                return DispatchResult(
                    task.task_id,
                    "passed",
                    "control passed",
                    changed_files=changed,
                    verify=last_verify,
                    control_verdict=verdict,
                    rounds=rnd,
                )
            if decision == "block":
                return DispatchResult(
                    task.task_id,
                    "blocked",
                    "control blocked",
                    changed_files=changed,
                    verify=last_verify,
                    control_verdict=verdict,
                    rounds=rnd,
                )
            # needs_changes, or a "pass" the controller flagged not_ready
            if decision in {"needs_changes", "pass"} and rnd < self.config.max_correction_rounds:
                fixes = "; ".join(str(f) for f in verdict.get("required_fixes", []) or [])
                active_packet = self._write_correction(
                    task, run_dir, rnd, fixes or "see control review"
                )
                continue
            return DispatchResult(
                task.task_id,
                "failed",
                "control did not pass",
                changed_files=changed,
                verify=last_verify,
                control_verdict=verdict,
                rounds=rnd,
            )
        return DispatchResult(task.task_id, "failed", "correction rounds exhausted")

    # --- helpers ------------------------------------------------------------
    def _allowed_globs(self, task_packet: Path) -> list[str]:
        fields = packets.parse_fields(task_packet.read_text(encoding="utf-8"))
        globs = parse_scope_globs(fields.get("RELATED_FILES", ""))
        globs.append(f"{self.config.handoffs_dirname}/**")
        return globs

    def _write_correction(self, task: Task, run_dir: Path, rnd: int, reason: str) -> Path:
        packet = packets.scaffold_packet(
            self.config.handoffs_dir,
            dispatch_id=task.task_id,
            packet_type="CORRECT",
            author="controller:codex",
        )
        text = packet.read_text(encoding="utf-8")
        packet.write_text(
            text + f"\n## Required fixes (round {rnd})\n\n{reason}\n", encoding="utf-8"
        )
        (run_dir / f"correction.round{rnd}.txt").write_text(reason, encoding="utf-8")
        return packet

    # --- prompt builders (functional, refine over time) ---------------------
    def _plan_prompt(self, task: Task, task_packet: Path, revision: int) -> str:
        rel = task_packet.relative_to(self.config.repo_root)
        note = (
            ""
            if revision == 0
            else " The prior draft failed validation; fill every field concretely this time."
        )
        return (
            f"You are the PLANNER. Return ONLY the JSON object required by the output schema "
            f"for TASK packet `{rel}`. Do not edit files or run commands.{note}\n\n"
            f"Task {task.task_id}: {task.title}\n\n{task.body}\n\n"
            "The `dispatch_id` must exactly match the task id. `related_files` must list the "
            "exact repo-relative paths or globs the executor MAY edit; keep it minimal. `goal` "
            "should be one concise paragraph. `notes` should include any acceptance criteria "
            "the executor and controller need."
        )

    def _gate_prompt(self, task_packet: Path) -> str:
        rel = task_packet.relative_to(self.config.repo_root)
        return (
            f"You are the PLAN GATE (read-only). Review the TASK packet `{rel}` for scope, "
            "safety, and clarity. Do not edit anything. End your reply with exactly one line:\n"
            "GATE_VERDICT: approve   (or) GATE_VERDICT: needs_changes   (or) GATE_VERDICT: block"
        )

    def _exec_prompt(self, active_packet: Path, rnd: int) -> str:
        rel = active_packet.relative_to(self.config.repo_root)
        return (
            f"You are the EXECUTOR. Carry out the packet `{rel}`, editing only files within "
            "its declared scope. When done, write an EXECUTION_REPORT (EXEC) handoff packet "
            "and end your reply with:\n"
            "EXEC_STATUS: executed   (or) EXEC_STATUS: blocked   (or) EXEC_STATUS: failed\n"
            "EXEC_PACKET: <repo-relative path to the EXEC packet you wrote>"
        )

    def _control_prompt(self, task_packet: Path, changed: tuple[str, ...]) -> str:
        rel = task_packet.relative_to(self.config.repo_root)
        files = ", ".join(changed) or "(none)"
        return (
            f"You are the CONTROLLER (read-only, independent). Review the change made under "
            f"TASK packet `{rel}`. Inspect it with `git diff HEAD` and `git status` for tracked "
            "edits, and read any newly-added (untracked) files directly — plain `git diff` will "
            f"be empty for added files. Changed files: {files}. Judge correctness, scope, and "
            "whether it is ready to publish. Return ONLY the JSON object required by the control "
            "schema."
        )
