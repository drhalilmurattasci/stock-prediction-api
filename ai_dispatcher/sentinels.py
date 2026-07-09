"""Fail-closed autonomy sentinels: kill switch, halt, breaker, seatbelt.

These implement the "stop rather than barrel on" posture from the RGE queue.
All state is small JSON/text files under the dispatcher's state directory so an
operator can inspect or clear them by hand.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from ai_dispatcher.config import DispatcherConfig


@dataclass(frozen=True)
class HaltState:
    halted: bool
    reason: str | None


def _read_count(path_text: str | None) -> int:
    if not path_text:
        return 0
    try:
        data = json.loads(path_text)
    except json.JSONDecodeError:
        return 0
    value = data.get("count", 0) if isinstance(data, dict) else 0
    return int(value) if isinstance(value, int) else 0


def is_stopped(config: DispatcherConfig) -> bool:
    """True if the always-on operator kill switch is present."""
    return config.stop_sentinel.exists()


def halt_state(config: DispatcherConfig) -> HaltState:
    """Read the durable halt sentinel."""
    path = config.halt_sentinel
    if not path.exists():
        return HaltState(halted=False, reason=None)
    reason = path.read_text(encoding="utf-8").strip() or None
    return HaltState(halted=True, reason=reason)


def write_halt(config: DispatcherConfig, reason: str) -> None:
    """Write the durable halt sentinel; a human clears it to resume."""
    config.ai_dir.mkdir(parents=True, exist_ok=True)
    config.halt_sentinel.write_text(reason, encoding="utf-8")


def clear_halt(config: DispatcherConfig) -> None:
    config.halt_sentinel.unlink(missing_ok=True)


def consecutive_failures(config: DispatcherConfig) -> int:
    path = config.failure_counter
    return _read_count(path.read_text(encoding="utf-8") if path.exists() else None)


def bump_consecutive_failures(config: DispatcherConfig) -> int:
    count = consecutive_failures(config) + 1
    config.ai_dir.mkdir(parents=True, exist_ok=True)
    config.failure_counter.write_text(json.dumps({"count": count}), encoding="utf-8")
    return count


def reset_consecutive_failures(config: DispatcherConfig) -> None:
    config.failure_counter.write_text(json.dumps({"count": 0}), encoding="utf-8")


def breaker_tripped(config: DispatcherConfig) -> bool:
    """True once the consecutive-failure count reaches the configured cap."""
    cap = config.max_consecutive_failures
    return cap > 0 and consecutive_failures(config) >= cap


def bump_seatbelt(config: DispatcherConfig) -> int:
    path = config.seatbelt_counter
    count = _read_count(path.read_text(encoding="utf-8") if path.exists() else None) + 1
    config.ai_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"count": count}), encoding="utf-8")
    return count


def seatbelt_due(config: DispatcherConfig) -> bool:
    """True when filed-since-review has reached the seatbelt interval."""
    interval = config.seatbelt_interval
    path = config.seatbelt_counter
    count = _read_count(path.read_text(encoding="utf-8") if path.exists() else None)
    return interval > 0 and count >= interval


def reset_seatbelt(config: DispatcherConfig) -> None:
    config.seatbelt_counter.write_text(json.dumps({"count": 0}), encoding="utf-8")
