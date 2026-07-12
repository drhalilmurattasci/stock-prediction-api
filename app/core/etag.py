"""Deterministic HTTP entity-tag helpers for cache revalidation."""

from __future__ import annotations

import hashlib
import re

_ENTITY_TAG = re.compile(r'(?:W/)?"[\x21\x23-\x7e\x80-\xff]*"')


def strong_etag(payload: bytes) -> str:
    """Return a quoted strong ETag derived from the exact response bytes."""
    return f'"{hashlib.sha256(payload).hexdigest()}"'


def if_none_match_matches(value: str | None, current_etag: str) -> bool:
    """Apply weak comparison semantics to a syntactically valid header value."""
    if value is None:
        return False
    candidates = _parse_if_none_match(value)
    if candidates is None:
        return False
    current = _without_weak_prefix(current_etag)
    return any(
        candidate == "*" or _without_weak_prefix(candidate) == current for candidate in candidates
    )


def _parse_if_none_match(value: str) -> tuple[str, ...] | None:
    """Parse ``*`` or an RFC-style comma-separated entity-tag list strictly."""
    text = value.strip()
    if text == "*":
        return ("*",)
    if not text:
        return None

    tags: list[str] = []
    position = 0
    while position < len(text):
        match = _ENTITY_TAG.match(text, position)
        if match is None:
            return None
        tags.append(match.group(0))
        position = match.end()
        while position < len(text) and text[position] in " \t":
            position += 1
        if position == len(text):
            break
        if text[position] != ",":
            return None
        position += 1
        while position < len(text) and text[position] in " \t":
            position += 1
        if position == len(text):
            return None
    return tuple(tags)


def _without_weak_prefix(etag: str) -> str:
    value = etag.strip()
    if value.startswith("W/"):
        return value[2:].lstrip()
    return value
