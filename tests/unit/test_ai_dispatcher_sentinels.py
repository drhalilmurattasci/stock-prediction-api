"""Sentinel tests: halt, kill switch, failure breaker, seatbelt."""

from __future__ import annotations

from pathlib import Path

from ai_dispatcher import sentinels
from ai_dispatcher.config import default_config


def test_halt_write_read_clear(tmp_path: Path) -> None:
    config = default_config(tmp_path)
    assert not sentinels.halt_state(config).halted
    sentinels.write_halt(config, "seatbelt: review needed")
    state = sentinels.halt_state(config)
    assert state.halted
    assert state.reason == "seatbelt: review needed"
    sentinels.clear_halt(config)
    assert not sentinels.halt_state(config).halted


def test_stop_sentinel(tmp_path: Path) -> None:
    config = default_config(tmp_path)
    assert not sentinels.is_stopped(config)
    config.ai_dir.mkdir(parents=True, exist_ok=True)
    config.stop_sentinel.write_text("stop", encoding="utf-8")
    assert sentinels.is_stopped(config)


def test_failure_breaker_trips_at_cap(tmp_path: Path) -> None:
    config = default_config(tmp_path).with_overrides(max_consecutive_failures=3)
    assert not sentinels.breaker_tripped(config)
    for _ in range(3):
        sentinels.bump_consecutive_failures(config)
    assert sentinels.breaker_tripped(config)
    sentinels.reset_consecutive_failures(config)
    assert not sentinels.breaker_tripped(config)


def test_seatbelt_due_at_interval(tmp_path: Path) -> None:
    config = default_config(tmp_path).with_overrides(seatbelt_interval=2)
    assert not sentinels.seatbelt_due(config)
    sentinels.bump_seatbelt(config)
    assert not sentinels.seatbelt_due(config)
    sentinels.bump_seatbelt(config)
    assert sentinels.seatbelt_due(config)
    sentinels.reset_seatbelt(config)
    assert not sentinels.seatbelt_due(config)
