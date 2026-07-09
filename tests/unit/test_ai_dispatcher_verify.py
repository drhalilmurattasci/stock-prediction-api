"""Verify-gate tests, including step-count enforcement (the audit fix)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ai_dispatcher.config import default_config
from ai_dispatcher.subprocess_utils import CommandResult
from ai_dispatcher.verify import is_publishable, run_verify


def _make_runner(fail_label: str | None = None):
    def runner(argv: Sequence[str], **_: Any) -> CommandResult:
        argv_t = tuple(argv)
        label = " ".join(argv)
        code = 1 if (fail_label is not None and fail_label in label) else 0
        return CommandResult(argv=argv_t, exit_code=code, stdout="", stderr="", duration_s=0.1)

    return runner


def test_verify_all_green_is_publishable(tmp_path: Path) -> None:
    config = default_config(tmp_path)
    result = run_verify(config, runner=_make_runner())
    assert result.ok
    assert result.steps_run == result.steps_total == 4
    assert is_publishable(result)


def test_verify_fails_fast_and_reports_step(tmp_path: Path) -> None:
    config = default_config(tmp_path)
    result = run_verify(config, runner=_make_runner(fail_label="mypy"))
    assert not result.ok
    assert result.failed_label == "mypy"
    # fail-fast: ruff check + ruff format + mypy ran, pytest did not
    assert result.steps_run == 3
    assert not is_publishable(result)


def test_trimmed_step_list_cannot_self_certify(tmp_path: Path) -> None:
    # A shortened gate that "passes" must still be rejected for publish.
    config = default_config(tmp_path).with_overrides(
        verify_steps=(("ruff check", ("uv", "run", "ruff", "check", ".")),)
    )
    result = run_verify(config, runner=_make_runner())
    assert result.ok  # the single step passed...
    assert not is_publishable(result)  # ...but the trimmed set is not publishable
