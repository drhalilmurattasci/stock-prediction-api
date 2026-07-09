"""CI-parity verification gate — the load-bearing deterministic protection.

The steps mirror ``.github/workflows/ci.yml`` (the ``lint-type-test`` job)
one-for-one, so ``ok`` means "CI would pass". Two anti-self-certification
guards, aimed squarely at the RGE audit's finding that a bare ``VERIFY OK``
string with no step accounting let a trimmed gate certify itself:

1. the result records ``steps_run`` and ``steps_total``; the publisher requires
   ``steps_run == steps_total`` (fail-fast means a failure leaves them unequal);
2. :func:`is_publishable` additionally requires at least :data:`MIN_VERIFY_STEPS`
   configured steps, so a *shortened* step list cannot pass either.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ai_dispatcher.config import DispatcherConfig
from ai_dispatcher.subprocess_utils import CommandResult, Runner, run_command

#: The verify config must declare at least this many steps to be publishable.
#: Matches the four CI steps (ruff check, ruff format --check, mypy, pytest).
MIN_VERIFY_STEPS = 4


@dataclass(frozen=True)
class VerifyStepResult:
    label: str
    exit_code: int
    duration_s: float
    timed_out: bool
    stalled: bool

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out and not self.stalled


@dataclass(frozen=True)
class VerifyResult:
    ok: bool
    steps_total: int
    steps_run: int
    failed_label: str | None
    steps: list[VerifyStepResult] = field(default_factory=list)

    def summary_line(self) -> str:
        if self.ok:
            return f"VERIFY OK: all {self.steps_total} verification step(s) passed."
        if self.failed_label is not None:
            return f"VERIFY FAIL: {self.failed_label}"
        return "VERIFY FAIL: no steps ran"


def run_verify(
    config: DispatcherConfig,
    *,
    runner: Runner = run_command,
    log_sink: list[str] | None = None,
) -> VerifyResult:
    """Run the verify steps in order, fail-fast, and return a structured result.

    ``runner`` is injectable so the gate can be unit-tested without a live
    toolchain. ``log_sink`` collects human-readable lines for a run log.
    """
    steps = config.verify_steps
    results: list[VerifyStepResult] = []

    def emit(line: str) -> None:
        if log_sink is not None:
            log_sink.append(line)

    for index, (label, argv) in enumerate(steps, start=1):
        emit(f"=== [{index}/{len(steps)}] {label} ===")
        outcome: CommandResult = runner(
            list(argv), timeout_s=config.verify_timeout_s, cwd=config.repo_root
        )
        step = VerifyStepResult(
            label=label,
            exit_code=outcome.exit_code,
            duration_s=outcome.duration_s,
            timed_out=outcome.timed_out,
            stalled=outcome.stalled,
        )
        results.append(step)
        if not step.ok:
            emit(f"--- STEP FAILED: {label} (exit {step.exit_code}) ---")
            result = VerifyResult(
                ok=False,
                steps_total=len(steps),
                steps_run=len(results),
                failed_label=label,
                steps=results,
            )
            emit(result.summary_line())
            return result
        emit(f"--- ok: {label} ({step.duration_s:.0f}s) ---")

    result = VerifyResult(
        ok=True,
        steps_total=len(steps),
        steps_run=len(results),
        failed_label=None,
        steps=results,
    )
    emit(result.summary_line())
    return result


def is_publishable(result: VerifyResult) -> bool:
    """Gate a run for publish: all steps green AND the step set was not trimmed."""
    return (
        result.ok
        and result.steps_run == result.steps_total
        and result.steps_total >= MIN_VERIFY_STEPS
    )
