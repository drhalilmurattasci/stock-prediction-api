"""Handoff-packet protocol tests."""

from __future__ import annotations

from pathlib import Path

from ai_dispatcher import packets

VALID_PACKET = """# TASK — demo

- DISPATCH_ID: demo
- AUTHOR: planner:codex
- TIMESTAMP: 2026-07-09_10-00-00+0300
- RELATED_FILES: `docs/**`
- STATUS: ready

## Goal

Do the thing.

- HANDOFF_STATUS: COMPLETE
- DISPATCH_ID: demo
- NEXT_ROLE: EXECUTOR_AI
- EXIT_CODE: 0
"""


def test_scaffold_packet_starts_invalid_due_to_placeholders(tmp_path: Path) -> None:
    path = packets.scaffold_packet(
        tmp_path, dispatch_id="demo", packet_type="TASK", author="planner:codex"
    )
    assert path.exists()
    problems = packets.validate_packet(path)
    assert any("RELATED_FILES" in p for p in problems)


def test_validate_accepts_filled_packet(tmp_path: Path) -> None:
    path = tmp_path / "demo_TASK_2026-07-09_10-00-00+0300.md"
    path.write_text(VALID_PACKET, encoding="utf-8")
    assert packets.validate_packet(path) == []


def test_extract_packet_markdown_accepts_fenced_packet() -> None:
    text = f"```markdown\n{VALID_PACKET}\n```"
    assert (
        packets.extract_packet_markdown(text, packet_type="TASK", dispatch_id="demo")
        == f"{VALID_PACKET.strip()}\n"
    )


def test_extract_packet_markdown_strips_preamble() -> None:
    text = f"Here is the packet:\n\n{VALID_PACKET}\n"
    assert (
        packets.extract_packet_markdown(text, packet_type="TASK", dispatch_id="demo")
        == f"{VALID_PACKET.strip()}\n"
    )


def test_finalize_writes_sidecar_and_dry_run_does_not(tmp_path: Path) -> None:
    path = tmp_path / "demo_TASK_x.md"
    path.write_text(VALID_PACKET, encoding="utf-8")

    assert packets.finalize_packet(path, packet_type="TASK", dry_run=True) == []
    assert not packets.is_finalized(path)

    assert packets.finalize_packet(path, packet_type="TASK") == []
    assert packets.is_finalized(path)
    assert packets.sidecar_path(path).exists()


def test_finalize_refuses_invalid_packet(tmp_path: Path) -> None:
    path = packets.scaffold_packet(tmp_path, dispatch_id="demo", packet_type="TASK", author="a")
    problems = packets.finalize_packet(path, packet_type="TASK")
    assert problems
    assert not packets.is_finalized(path)


def test_latest_packet_returns_newest(tmp_path: Path) -> None:
    first = tmp_path / "demo_EXEC_2026-07-09_10-00-00+0300.md"
    second = tmp_path / "demo_EXEC_2026-07-09_11-00-00+0300.md"
    first.write_text(VALID_PACKET, encoding="utf-8")
    second.write_text(VALID_PACKET, encoding="utf-8")
    import os

    os.utime(second, (10**9 + 100, 10**9 + 100))
    os.utime(first, (10**9, 10**9))
    latest = packets.latest_packet(tmp_path, dispatch_id="demo", packet_type="EXEC")
    assert latest == second
