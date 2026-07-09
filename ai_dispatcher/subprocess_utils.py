"""Bounded subprocess execution with timeout, stall detection, and tree-kill.

This replaces the RGE loop's child-powershell + ``taskkill /T`` machinery. In
Python the whole PowerShell-5.1 hazard class disappears: an exit code is an
integer, stderr is just text (never a terminating error), and we kill the whole
process *group* so a hung ``codex``/``claude``/``uv`` cannot wedge the pipeline.

Exit-code contract (mirrors the RGE loop):
* normal completion -> the child's own exit code
* wall-clock timeout -> :data:`~ai_dispatcher.config.EXIT_TIMEOUT` (124), ``timed_out=True``
* output-stall       -> :data:`~ai_dispatcher.config.EXIT_STALL` (125), ``stalled=True``

The platform branches below are written with direct ``sys.platform`` checks so
mypy prunes the inapplicable branch on each OS (POSIX ``os.killpg`` /
``SIGKILL`` vs Windows ``CREATE_NEW_PROCESS_GROUP`` / ``taskkill``).
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import IO

from ai_dispatcher.config import EXIT_STALL, EXIT_TIMEOUT

_POLL_INTERVAL_S = 0.1


@dataclass(frozen=True)
class CommandResult:
    """Outcome of a bounded subprocess run."""

    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str
    duration_s: float
    timed_out: bool = False
    stalled: bool = False

    @property
    def ok(self) -> bool:
        """True only on a clean zero exit (not a timeout/stall)."""
        return self.exit_code == 0 and not self.timed_out and not self.stalled


#: A callable with the shape of :func:`run_command`, for dependency injection.
Runner = Callable[..., CommandResult]


def _spawn(
    argv: Sequence[str], *, cwd: str | None, env: Mapping[str, str] | None
) -> subprocess.Popen[str]:
    env_arg = dict(env) if env is not None else None
    if sys.platform == "win32":
        return subprocess.Popen(
            list(argv),
            cwd=cwd,
            env=env_arg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        )
    return subprocess.Popen(
        list(argv),
        cwd=cwd,
        env=env_arg,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
    )


def _kill_tree(proc: subprocess.Popen[str]) -> None:
    """Kill the child and every descendant, cross-platform."""
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
            capture_output=True,
            check=False,
        )
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        proc.kill()


def run_command(
    argv: Sequence[str],
    *,
    timeout_s: float,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    stdin: str | None = None,
    stall_threshold_s: float = 0.0,
) -> CommandResult:
    """Run ``argv`` under a hard timeout and optional output-stall watchdog.

    ``stall_threshold_s`` > 0 arms a watchdog that fires once output has begun
    and then stops growing for that many seconds — matching the RGE design
    where slow *startup* never counts as a stall.
    """
    argv_t = tuple(argv)
    started = time.monotonic()
    proc = _spawn(argv_t, cwd=os.fspath(cwd) if cwd is not None else None, env=env)

    out_chunks: list[str] = []
    err_chunks: list[str] = []
    last_activity = started
    lock = threading.Lock()

    def _drain(stream: IO[str] | None, sink: list[str]) -> None:
        nonlocal last_activity
        if stream is None:
            return
        for line in stream:
            with lock:
                sink.append(line)
                last_activity = time.monotonic()

    # Start draining stdout/stderr BEFORE writing stdin: a large prompt to a
    # child that is also producing output would otherwise deadlock (child blocks
    # on a full stdout pipe while we block writing stdin).
    readers = [
        threading.Thread(target=_drain, args=(proc.stdout, out_chunks), daemon=True),
        threading.Thread(target=_drain, args=(proc.stderr, err_chunks), daemon=True),
    ]
    for reader in readers:
        reader.start()

    if stdin is not None and proc.stdin is not None:
        try:
            proc.stdin.write(stdin)
        except OSError:
            pass  # child exited early / closed stdin; its exit code is the signal
        finally:
            with contextlib.suppress(OSError):
                proc.stdin.close()

    timed_out = False
    stalled = False
    while proc.poll() is None:
        now = time.monotonic()
        if now - started >= timeout_s:
            timed_out = True
            _kill_tree(proc)
            break
        if stall_threshold_s > 0:
            with lock:
                idle = now - last_activity
                had_output = bool(out_chunks or err_chunks)
            if had_output and idle >= stall_threshold_s:
                stalled = True
                _kill_tree(proc)
                break
        time.sleep(_POLL_INTERVAL_S)

    proc.wait()
    for reader in readers:
        reader.join(timeout=2.0)

    raw_exit = proc.returncode if proc.returncode is not None else 1
    if timed_out:
        exit_code = EXIT_TIMEOUT
    elif stalled:
        exit_code = EXIT_STALL
    else:
        exit_code = raw_exit

    return CommandResult(
        argv=argv_t,
        exit_code=exit_code,
        stdout="".join(out_chunks),
        stderr="".join(err_chunks),
        duration_s=time.monotonic() - started,
        timed_out=timed_out,
        stalled=stalled,
    )
