"""Scope-guard tests: glob matching, status parsing, out-of-scope detection."""

from __future__ import annotations

from ai_dispatcher import scope_guard


def test_glob_matching_spans_and_bounds() -> None:
    assert scope_guard.matches_any("app/db/models/bars.py", ["app/**"])
    assert scope_guard.matches_any("pyproject.toml", ["pyproject.toml"])
    assert scope_guard.matches_any("docs/guide.md", ["*.md", "docs/**"])
    # single-star must not span a directory separator
    assert not scope_guard.matches_any("app/db/x.py", ["app/*.py"])
    assert not scope_guard.matches_any("ingestion/x.py", ["app/**"])


def test_parse_status_porcelain_handles_rename_and_untracked() -> None:
    text = " M app/main.py\n?? ai_dispatcher/handoffs/x.md\nR  old.py -> new/name.py\n"
    paths = scope_guard.parse_status_porcelain(text)
    assert paths == {"app/main.py", "ai_dispatcher/handoffs/x.md", "new/name.py"}


def test_out_of_scope_flags_only_disallowed_paths() -> None:
    changed = ["docs/a.md", "app/secret.py", "tests/test_x.py"]
    allowed = ["docs/**", "tests/**"]
    assert scope_guard.out_of_scope(changed, allowed) == ["app/secret.py"]


def test_parse_scope_globs_splits_backtick_and_commas() -> None:
    assert scope_guard.parse_scope_globs("`app/**`, `tests/**`") == ["app/**", "tests/**"]
    assert scope_guard.parse_scope_globs("") == []
