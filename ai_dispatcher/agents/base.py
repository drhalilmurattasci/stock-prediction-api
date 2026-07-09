"""Shared agent types and tail-anchored decision-marker extraction.

Model prose is untrusted. Rather than trust a JSON blob embedded in prose, the
loop reads a small set of line-anchored ``NAME: value`` decision markers from
only the *tail* of the output — so a marker quoted mid-explanation ("I will end
with ``GATE_VERDICT: approve``") cannot be misread as the actual verdict. This
mirrors the RGE loop's tail-anchoring defense.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field

_MARKER_RE = re.compile(r"^([A-Z][A-Z0-9_]*):[ \t]?(.*)$")
_DEFAULT_TAIL_LINES = 8


@dataclass(frozen=True)
class AgentResult:
    """Outcome of one model invocation."""

    ok: bool
    text: str
    markers: dict[str, str] = field(default_factory=dict)
    raw: str = ""
    error: str | None = None


def extract_markers(
    text: str, names: Iterable[str], *, tail_lines: int = _DEFAULT_TAIL_LINES
) -> dict[str, str]:
    """Extract the last ``NAME: value`` for each requested name from the tail.

    Only the final ``tail_lines`` non-empty lines are considered. For each name
    the *last* matching occurrence wins (models sometimes restate the marker).
    """
    wanted = set(names)
    non_empty = [ln.rstrip("\r\n") for ln in text.splitlines() if ln.strip()]
    tail = non_empty[-tail_lines:]
    found: dict[str, str] = {}
    for line in tail:
        match = _MARKER_RE.match(line)
        if match is None:
            continue
        key, value = match.group(1), match.group(2).strip()
        if key in wanted:
            found[key] = value
    return found
