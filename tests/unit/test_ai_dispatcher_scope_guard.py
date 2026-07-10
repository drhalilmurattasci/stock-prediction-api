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


def test_glob_does_not_over_match_across_newlines() -> None:
    # defense-in-depth: a wildcard must not consume a newline, and the anchor
    # must not match around a trailing newline.
    assert not scope_guard.matches_any("secret.py\nallowed.md", ["*.md"])
    assert not scope_guard.matches_any("app/x.py\nevil", ["app/**"])
    assert not scope_guard.matches_any("docs/a.md\n", ["docs/**"])
    # legit matches still hold
    assert scope_guard.matches_any("docs/a.md", ["docs/**"])
    assert scope_guard.matches_any("x.md", ["*.md"])


def test_parse_status_porcelain_handles_rename_and_untracked() -> None:
    text = " M app/main.py\n?? ai_dispatcher/handoffs/x.md\nR  old.py -> new/name.py\n"
    paths = scope_guard.parse_status_porcelain(text)
    assert paths == {"app/main.py", "ai_dispatcher/handoffs/x.md", "new/name.py"}


def test_parse_status_keeps_arrow_in_non_rename_filenames() -> None:
    # H2: only R/C entries are "ORIG -> PATH". A modified/untracked file whose
    # literal name contains " -> " must NOT be split down to its suffix, which
    # would evade the scope guard / authorization coverage on the real path.
    text = (
        " M weird -> name.py\n"  # modified file literally named "weird -> name.py"
        "?? docs/a -> b.md\n"  # untracked file with an arrow in its name
        "R  old.py -> new/name.py\n"  # a genuine rename still yields the destination
    )
    paths = scope_guard.parse_status_porcelain(text)
    assert paths == {"weird -> name.py", "docs/a -> b.md", "new/name.py"}


def test_out_of_scope_flags_only_disallowed_paths() -> None:
    changed = ["docs/a.md", "app/secret.py", "tests/test_x.py"]
    allowed = ["docs/**", "tests/**"]
    assert scope_guard.out_of_scope(changed, allowed) == ["app/secret.py"]


def test_parse_scope_globs_splits_backtick_and_commas() -> None:
    assert scope_guard.parse_scope_globs("`app/**`, `tests/**`") == ["app/**", "tests/**"]
    assert scope_guard.parse_scope_globs("") == []
