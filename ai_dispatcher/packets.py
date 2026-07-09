"""Handoff-packet protocol: append-only markdown packets + JSON sidecars.

A packet is a markdown file ``<dispatch_id>_<TYPE>_<timestamp>.md`` under the
handoffs directory. A packet is *finalized* (== approved / accepted) when its
``.meta.json`` sidecar exists — that sidecar is the only signal ``--resume``
trusts, exactly as in the RGE protocol.

Three packet types are used by the loop:

* ``TASK``    — the bounded spec the planner produced and the gate approved.
* ``EXEC``    — the executor's report of what it changed.
* ``CORRECT`` — a correction directive routed back to the executor.

Validation is deliberately strict and mechanical (no model in the loop): the
header block and the machine footer must carry non-placeholder values.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

PacketType = Literal["TASK", "EXEC", "CORRECT"]

_REQUIRED_HEADER = ("DISPATCH_ID", "AUTHOR", "TIMESTAMP", "RELATED_FILES", "STATUS")
_REQUIRED_FOOTER = ("HANDOFF_STATUS", "DISPATCH_ID", "NEXT_ROLE", "EXIT_CODE")
_FIELD_RE = re.compile(r"^-[ \t]+([A-Z][A-Z0-9_]*):[ \t]*(.*)$", re.MULTILINE)
_FENCE_RE = re.compile(r"```(?:markdown|md)?[ \t]*\r?\n(.*?)\r?\n```", re.DOTALL | re.I)
_PLACEHOLDER_RE = re.compile(r"<[^>]+>|\bTODO\b|\bFIXME\b|\bTBD\b")
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")
_RELATED_FILE_DELIMITER_RE = re.compile(r"[`,\s]")


@dataclass(frozen=True)
class PacketRef:
    """A packet file on disk plus its parsed fields."""

    path: Path
    packet_type: PacketType
    fields: dict[str, str]

    @property
    def finalized(self) -> bool:
        return sidecar_path(self.path).exists()


def _timestamp(now: datetime | None = None) -> str:
    stamp = (now or datetime.now().astimezone()).strftime("%Y-%m-%d_%H-%M-%S%z")
    return stamp


def sidecar_path(packet_path: Path) -> Path:
    """Return the ``.meta.json`` sidecar path for a packet."""
    return packet_path.with_suffix(".meta.json")


def parse_fields(text: str) -> dict[str, str]:
    """Parse ``- KEY: value`` lines into a dict (last write wins)."""
    return {m.group(1): m.group(2).strip() for m in _FIELD_RE.finditer(text)}


def _escape_packet_field_lines(text: str) -> str:
    """Keep free text from being parsed as packet header/footer fields."""
    return "\n".join(f"> {line}" if _FIELD_RE.match(line) else line for line in text.splitlines())


def extract_packet_markdown(text: str, *, packet_type: PacketType, dispatch_id: str) -> str:
    """Extract packet markdown from a model final message.

    The planner is read-only and returns the completed TASK packet as text. In
    practice models may wrap it in a markdown fence or add a short preamble; we
    accept only content that contains the requested packet header and dispatch
    id. Validation still decides whether the extracted packet is usable.
    """
    candidates = [match.group(1).strip() for match in _FENCE_RE.finditer(text)]
    candidates.append(text.strip())
    header = f"# {packet_type}"
    for candidate in candidates:
        start = candidate.find(header)
        if start == -1:
            continue
        packet = candidate[start:].strip()
        fields = parse_fields(packet)
        if fields.get("DISPATCH_ID") == dispatch_id:
            return f"{packet}\n"
    return f"{text.strip()}\n"


def render_task_packet_from_plan_json(plan_text: str, *, scaffold_path: Path) -> str:
    """Render canonical TASK markdown from planner JSON.

    Codex planning uses ``--output-schema`` so the model decides bounded intent
    and scope, while this code owns the wire format. That prevents live model
    drift from breaking packet headers/footers.
    """
    try:
        data = json.loads(plan_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid planner JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("planner JSON must be an object")

    scaffold_fields = parse_fields(scaffold_path.read_text(encoding="utf-8"))
    dispatch_id = str(data.get("dispatch_id", "")).strip()
    if dispatch_id != scaffold_fields.get("DISPATCH_ID"):
        raise ValueError("planner JSON dispatch_id does not match scaffold")
    related = data.get("related_files")
    if not isinstance(related, list) or not related:
        raise ValueError("planner JSON related_files must be a non-empty list")
    related_files = [str(item).strip() for item in related]
    if any(not item for item in related_files):
        raise ValueError("planner JSON related_files cannot contain empty values")
    if any(_RELATED_FILE_DELIMITER_RE.search(item) for item in related_files):
        raise ValueError("planner JSON related_files entries must be single scope tokens")
    goal = str(data.get("goal", "")).strip()
    if not goal:
        raise ValueError("planner JSON goal cannot be empty")
    notes = str(data.get("notes", "")).strip() or "No additional notes."
    goal = _escape_packet_field_lines(goal)
    notes = _escape_packet_field_lines(notes)
    related_inline = ", ".join(f"`{item}`" for item in related_files)
    author = scaffold_fields.get("AUTHOR", "planner:codex")
    timestamp = scaffold_fields.get("TIMESTAMP", _timestamp())
    return f"""# TASK — {dispatch_id}

- DISPATCH_ID: {dispatch_id}
- AUTHOR: {author}
- TIMESTAMP: {timestamp}
- RELATED_FILES: {related_inline}
- STATUS: ready

## Goal

{goal}

## Scope

MAY edit: {related_inline}
MUST NOT edit: any file outside the RELATED_FILES list.

## Notes

{notes}

<!-- machine-footer: filled on completion -->
- HANDOFF_STATUS: COMPLETE
- DISPATCH_ID: {dispatch_id}
- NEXT_ROLE: EXECUTOR_AI
- EXIT_CODE: 0
"""


def scaffold_packet(
    handoffs_dir: Path,
    *,
    dispatch_id: str,
    packet_type: PacketType,
    author: str,
    now: datetime | None = None,
) -> Path:
    """Create a new packet file pre-filled with the required structure."""
    if not _NAME_RE.match(dispatch_id):
        raise ValueError(f"invalid dispatch id: {dispatch_id!r}")
    handoffs_dir.mkdir(parents=True, exist_ok=True)
    ts = _timestamp(now)
    path = handoffs_dir / f"{dispatch_id}_{packet_type}_{ts}.md"
    next_role = "CONTROLLER_AI" if packet_type == "EXEC" else "EXECUTOR_AI"
    body = _TEMPLATE.format(
        packet_type=packet_type,
        dispatch_id=dispatch_id,
        author=author,
        timestamp=ts,
        next_role=next_role,
    )
    path.write_text(body, encoding="utf-8")
    return path


def validate_packet(path: Path) -> list[str]:
    """Return a list of problems with a packet (empty list == valid)."""
    if not path.exists():
        return [f"packet not found: {path}"]
    text = path.read_text(encoding="utf-8")
    fields = parse_fields(text)
    problems: list[str] = []
    for key in _REQUIRED_HEADER:
        value = fields.get(key, "")
        if not value:
            problems.append(f"missing or empty header field: {key}")
        elif _PLACEHOLDER_RE.search(value):
            problems.append(f"header field {key} still holds a placeholder: {value!r}")
    for key in _REQUIRED_FOOTER:
        if key not in fields:
            problems.append(f"missing machine-footer field: {key}")
    exit_code = fields.get("EXIT_CODE", "")
    if exit_code and not exit_code.lstrip("-").isdigit():
        problems.append(f"EXIT_CODE must be an integer, got {exit_code!r}")
    return problems


def finalize_packet(path: Path, *, packet_type: PacketType, dry_run: bool = False) -> list[str]:
    """Validate a packet and, unless ``dry_run``, write its approval sidecar.

    Returns the list of validation problems; an empty list means it finalized
    (or would have, under ``dry_run``).
    """
    problems = validate_packet(path)
    if problems or dry_run:
        return problems
    fields = parse_fields(path.read_text(encoding="utf-8"))
    meta = {
        "packet_type": packet_type,
        "packet_file": path.name,
        "finalized_at": _timestamp(),
        "fields": fields,
    }
    sidecar_path(path).write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    return []


def is_finalized(path: Path) -> bool:
    """True if the packet's approval sidecar exists."""
    return sidecar_path(path).exists()


def latest_packet(handoffs_dir: Path, *, dispatch_id: str, packet_type: PacketType) -> Path | None:
    """Return the newest packet of a type for a dispatch id, or None."""
    if not handoffs_dir.exists():
        return None
    prefix = f"{dispatch_id}_{packet_type}_"
    matches = sorted(
        (p for p in handoffs_dir.glob(f"{prefix}*.md")),
        key=lambda p: p.stat().st_mtime,
    )
    return matches[-1] if matches else None


_TEMPLATE = """# {packet_type} — {dispatch_id}

- DISPATCH_ID: {dispatch_id}
- AUTHOR: {author}
- TIMESTAMP: {timestamp}
- RELATED_FILES: `<comma-separated repo-relative paths this packet concerns>`
- STATUS: draft

## Goal

<one-paragraph statement of the bounded task>

## Scope

MAY edit: `<glob>`, `<glob>`
MUST NOT edit: `<glob>`

## Notes

<planner/executor notes>

<!-- machine-footer: filled on completion -->
- HANDOFF_STATUS: DRAFT
- DISPATCH_ID: {dispatch_id}
- NEXT_ROLE: {next_role}
- EXIT_CODE: 0
"""
