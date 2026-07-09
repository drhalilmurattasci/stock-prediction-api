"""subprocess_utils: executable resolution + real run/timeout behavior."""

from __future__ import annotations

import sys

from ai_dispatcher.config import EXIT_TIMEOUT
from ai_dispatcher.subprocess_utils import _resolve_executable, run_command


def test_resolve_executable_falls_back_for_unknown() -> None:
    assert _resolve_executable("no-such-tool-xyz-123") == "no-such-tool-xyz-123"


def test_resolve_executable_resolves_a_full_path() -> None:
    # sys.executable is a real interpreter path; which() must resolve it.
    assert _resolve_executable(sys.executable)


def test_run_command_runs_and_captures_output() -> None:
    result = run_command([sys.executable, "-c", "print('hi-there')"], timeout_s=30)
    assert result.ok
    assert "hi-there" in result.stdout


def test_run_command_times_out_and_reports_124() -> None:
    result = run_command([sys.executable, "-c", "import time; time.sleep(5)"], timeout_s=0.5)
    assert result.timed_out
    assert result.exit_code == EXIT_TIMEOUT
    assert not result.ok
