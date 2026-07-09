"""Fail-closed scope guard: reject edits outside the task's declared surface.

The RGE audit found this to be one of only two genuinely load-bearing
protections. Before the executor runs we snapshot ``git status``; after, any
changed path that does not match the task's allowed globs is a scope violation
and the dispatch fails — a prompt-injection / scope-creep defense.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_STATUS_LINE_RE = re.compile(r"^..\s(?P<path>.+)$")


def _glob_to_regex(glob: str) -> re.Pattern[str]:
    escaped = re.escape(glob.replace("\\", "/"))
    # order matters: collapse ``**`` before single ``*``
    pattern = escaped.replace(r"\*\*", ".*").replace(r"\*", "[^/]*").replace(r"\?", "[^/]")
    return re.compile(f"^{pattern}$")


def matches_any(path: str, globs: Iterable[str]) -> bool:
    """True if ``path`` matches any of the glob patterns (``**`` spans dirs)."""
    normalized = path.replace("\\", "/")
    return any(_glob_to_regex(g).match(normalized) for g in globs)


def parse_status_porcelain(text: str) -> set[str]:
    """Parse ``git status --porcelain=v1`` output into a set of paths.

    Rename entries (``old -> new``) contribute the destination path.
    """
    paths: set[str] = set()
    for raw in text.splitlines():
        if not raw.strip():
            continue
        match = _STATUS_LINE_RE.match(raw)
        if match is None:
            continue
        path = match.group("path").strip()
        if " -> " in path:
            path = path.split(" -> ", 1)[1].strip()
        path = path.strip('"')
        if path:
            paths.add(path.replace("\\", "/"))
    return paths


def out_of_scope(changed_paths: Iterable[str], allowed_globs: Iterable[str]) -> list[str]:
    """Return changed paths that match none of the allowed globs, sorted."""
    allowed = list(allowed_globs)
    violations = [p for p in changed_paths if not matches_any(p, allowed)]
    return sorted(violations)


def parse_scope_globs(related_files_field: str) -> list[str]:
    """Parse a packet ``RELATED_FILES`` / MAY-edit field into glob patterns.

    Accepts backtick-wrapped, comma- or whitespace-separated entries.
    """
    tokens = re.split(r"[,\s]+", related_files_field.replace("`", " "))
    return [t.strip() for t in tokens if t.strip()]
