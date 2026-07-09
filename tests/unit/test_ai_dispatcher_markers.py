"""Tail-anchored decision-marker extraction tests."""

from __future__ import annotations

from ai_dispatcher.agents.base import extract_markers


def test_tail_anchoring_ignores_mid_prose_mention() -> None:
    text = (
        "I will finish by writing GATE_VERDICT: approve once I am done.\n"
        "Here is my analysis...\n"
        "GATE_VERDICT: block\n"
    )
    markers = extract_markers(text, ["GATE_VERDICT"])
    # the real verdict is the tail line, not the mid-prose mention
    assert markers["GATE_VERDICT"] == "block"


def test_last_occurrence_wins_and_only_requested_names() -> None:
    text = (
        "EXEC_STATUS: failed\nnote\n"
        "EXEC_STATUS: executed\nEXEC_PACKET: ai_dispatcher/handoffs/x.md\n"
    )
    markers = extract_markers(text, ["EXEC_STATUS", "EXEC_PACKET"])
    assert markers == {
        "EXEC_STATUS": "executed",
        "EXEC_PACKET": "ai_dispatcher/handoffs/x.md",
    }


def test_missing_marker_absent_from_result() -> None:
    assert extract_markers("no markers here\njust prose", ["GATE_VERDICT"]) == {}
