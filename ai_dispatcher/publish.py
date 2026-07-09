"""Publish layer: risk tiers, authorization ledger, and the merge/PR actions.

Publishing is deliberately separate from the loop (which never commits). The
decision is fail-closed: an auto-merge to ``main`` happens only when a recorded,
non-expired, non-exhausted authorization covers **every** changed file.
Anything else downgrades to a PR — the downgrade can never be promoted upward,
matching RGE's ladder and the audit's caution about unreviewed auto-merges.
"""

from __future__ import annotations

import json
import re
import warnings
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from ai_dispatcher.config import DispatcherConfig
from ai_dispatcher.scope_guard import matches_any
from ai_dispatcher.subprocess_utils import CommandResult, Runner, run_command

RiskTier = Literal["low", "high"]
PublishMode = Literal["branch", "pr", "main"]
PublishAction = Literal["branch", "pr", "merge"]

_AUTH_HEADING_RE = re.compile(
    r"^##[ \t]+AUTH[ \t]+(?P<id>[A-Za-z0-9][A-Za-z0-9_.\-]*)", re.MULTILINE
)
_AUTH_SECTION_RE = re.compile(r"^##[ \t]+AUTH\b.*$", re.MULTILINE)
_FIELD_RE = re.compile(r"^-[ \t]+([A-Z_]+):[ \t]*(.*)$", re.MULTILINE)
_HTML_COMMENT_TOKEN_RE = re.compile(r"<!--|-->")


@dataclass(frozen=True)
class Authorization:
    auth_id: str
    scope_globs: tuple[str, ...]
    max_merges: int
    expires: date | None
    granted_by: str

    def covers(self, changed_files: Sequence[str]) -> bool:
        """True only if EVERY changed file falls within this auth's scope."""
        return bool(changed_files) and all(matches_any(f, self.scope_globs) for f in changed_files)

    def expired(self, today: date) -> bool:
        return self.expires is not None and today > self.expires


@dataclass(frozen=True)
class PublishDecision:
    action: PublishAction
    reason: str
    auth_id: str | None = None


@dataclass(frozen=True)
class PublishOutcome:
    action: PublishAction
    ok: bool
    detail: str
    pr_url: str | None = None


def classify_risk(changed_files: Iterable[str], high_risk_globs: Iterable[str]) -> RiskTier:
    """High if any changed file matches a high-risk glob, else low."""
    globs = list(high_risk_globs)
    return "high" if any(matches_any(f, globs) for f in changed_files) else "low"


def parse_authorizations(text: str, *, today: date | None = None) -> list[Authorization]:
    """Parse the ledger; expired entries are dropped."""
    if _HTML_COMMENT_TOKEN_RE.search(text):
        warnings.warn(
            "HTML comments are not allowed in authorization ledger; ignoring all authorizations",
            RuntimeWarning,
            stacklevel=2,
        )
        return []
    out: list[Authorization] = []
    sections = list(_AUTH_SECTION_RE.finditer(text))
    for index, section in enumerate(sections):
        heading = _AUTH_HEADING_RE.match(section.group(0))
        if heading is None:
            continue
        start = section.end()
        end = sections[index + 1].start() if index + 1 < len(sections) else len(text)
        fields = {m.group(1): m.group(2).strip() for m in _FIELD_RE.finditer(text[start:end])}
        auth = _build_auth(heading.group("id"), fields)
        if auth is None:
            continue
        if today is not None and auth.expired(today):
            continue
        out.append(auth)
    return out


def _build_auth(auth_id: str, fields: Mapping[str, str]) -> Authorization | None:
    scope_raw = fields.get("SCOPE", "")
    scope = tuple(t.strip() for t in re.split(r"[,\s]+", scope_raw.replace("`", " ")) if t.strip())
    if not scope:
        return None
    max_merges_raw = fields.get("MAX_MERGES", "0").strip()
    if re.fullmatch(r"[0-9]+", max_merges_raw) is None:
        return None
    max_merges = int(max_merges_raw)
    if max_merges <= 0:
        return None
    expires_raw = fields.get("EXPIRES", "").strip()
    try:
        expires = date.fromisoformat(expires_raw) if expires_raw else None
    except ValueError:
        return None
    return Authorization(
        auth_id=auth_id,
        scope_globs=scope,
        max_merges=max_merges,
        expires=expires,
        granted_by=fields.get("GRANTED_BY", "unknown"),
    )


def decide_publish(
    mode: PublishMode,
    *,
    changed_files: Sequence[str],
    authorizations: Sequence[Authorization],
    used_counts: Mapping[str, int],
    high_risk_globs: Iterable[str],
    never_automerge_globs: Iterable[str] = (),
    artifact_globs: Iterable[str] = (),
) -> PublishDecision:
    """Decide how to publish a verified change, fail-closed toward PR."""
    if mode == "branch":
        return PublishDecision("branch", "branch mode: no remote publish")
    if not changed_files:
        return PublishDecision("branch", "no changes to publish")
    if mode == "pr":
        return PublishDecision("pr", "pr mode")
    # Dispatcher-generated audit artifacts (handoff packets) are not reviewable
    # product changes: they never trip the protected check and never need an
    # authorization to cover them.
    artifacts = list(artifact_globs)
    reviewable = [f for f in changed_files if not matches_any(f, artifacts)]
    if not reviewable:
        return PublishDecision("branch", "no reviewable changes (only dispatcher artifacts)")
    # mode == "main": never auto-merge self/CI-protected paths, even if authorized.
    protected = [f for f in reviewable if matches_any(f, list(never_automerge_globs))]
    if protected:
        return PublishDecision("pr", f"protected path requires review: {protected[0]}")
    # require a covering, non-exhausted authorization for every reviewable file.
    for auth in authorizations:
        if used_counts.get(auth.auth_id, 0) < auth.max_merges and auth.covers(reviewable):
            return PublishDecision("merge", f"authorized by {auth.auth_id}", auth.auth_id)
    risk = classify_risk(reviewable, high_risk_globs)
    return PublishDecision("pr", f"no covering authorization (risk={risk}); downgraded to PR")


# --- merge-count state ------------------------------------------------------
def _merges_log(config: DispatcherConfig) -> Path:
    return config.ai_dir / "merges.jsonl"


def merge_counts(config: DispatcherConfig) -> dict[str, int]:
    """Count prior auto-merges per authorization id from the merges log."""
    path = _merges_log(config)
    counts: dict[str, int] = {}
    if not path.exists():
        return counts
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        auth_id = row.get("auth_id") if isinstance(row, dict) else None
        if isinstance(auth_id, str):
            counts[auth_id] = counts.get(auth_id, 0) + 1
    return counts


def record_merge(config: DispatcherConfig, *, auth_id: str, dispatch_id: str, sha: str) -> None:
    config.ai_dir.mkdir(parents=True, exist_ok=True)
    row = {"auth_id": auth_id, "dispatch_id": dispatch_id, "sha": sha}
    with _merges_log(config).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


# --- git/gh actions (live; unit-tested only via an injected runner) ---------
@dataclass
class Publisher:
    """Performs the git/gh actions for a publish decision."""

    config: DispatcherConfig
    runner: Runner = run_command
    _log: list[str] = field(default_factory=list)

    def _git(self, *args: str, stdin: str | None = None) -> CommandResult:
        return self.runner(
            [*self.config.git, *args], timeout_s=300, cwd=self.config.repo_root, stdin=stdin
        )

    def _gh(self, *args: str) -> CommandResult:
        return self.runner([*self.config.gh, *args], timeout_s=300, cwd=self.config.repo_root)

    def publish(
        self,
        decision: PublishDecision,
        *,
        branch: str,
        commit_message: str,
        title: str,
        body: str,
        changed_files: Sequence[str] = (),
    ) -> PublishOutcome:
        if not self._create_branch(branch).ok:
            return PublishOutcome(decision.action, False, f"could not create branch {branch}")
        committed, detail = self._commit_exact(commit_message, expected=set(changed_files))
        if not committed:
            return PublishOutcome(decision.action, False, detail)
        if decision.action == "branch":
            return PublishOutcome("branch", True, f"committed on {branch} (local only)")
        if decision.action == "pr":
            return self._open_pr(branch, title, body)
        return self._merge_to_main(branch)

    def _create_branch(self, branch: str) -> CommandResult:
        # Carry the (uncommitted) working-tree changes onto a fresh branch cut
        # from the current clean base, so main is never committed to directly.
        return self._git("checkout", "-b", branch)

    def _commit_exact(self, message: str, *, expected: set[str]) -> tuple[bool, str]:
        """Stage and commit, refusing to include any file outside ``expected``.

        This is the last line of defense behind the scope guard: even if the
        loop miscomputed the change set, we never commit an unauthorized file.
        """
        if not self._git("add", "-A").ok:
            return False, "git add failed"
        staged = self._git("diff", "--cached", "--name-only")
        # Strip git's core.quotePath quoting so names match parse_status_porcelain
        # (which also strips surrounding quotes); both keep octal escapes, so the
        # representations line up for non-ASCII paths.
        staged_set = {
            line.strip().strip('"') for line in staged.stdout.splitlines() if line.strip()
        }
        extra = staged_set - expected
        if expected and extra:
            self._git("reset", "-q")
            return False, f"refusing to commit unauthorized files: {sorted(extra)}"
        if not self._git("commit", "-m", message).ok:
            return False, "git commit failed"
        return True, "committed"

    def _open_pr(self, branch: str, title: str, body: str) -> PublishOutcome:
        push = self._git("push", "-u", "origin", branch)
        if not push.ok:
            return PublishOutcome("pr", False, "push failed")
        created = self._gh(
            "pr", "create", "--base", "main", "--head", branch, "--title", title, "--body", body
        )
        url = (
            created.stdout.strip().splitlines()[-1]
            if created.ok and created.stdout.strip()
            else None
        )
        return PublishOutcome(
            "pr", created.ok, "opened PR" if created.ok else "gh pr create failed", pr_url=url
        )

    def _merge_to_main(self, branch: str) -> PublishOutcome:
        if not self._git("fetch", "origin", "main").ok:
            return PublishOutcome("merge", False, "fetch failed")
        if not self._git("checkout", "main").ok:
            return PublishOutcome("merge", False, "checkout main failed")
        if not self._git("merge", "--ff-only", branch).ok:
            return PublishOutcome("merge", False, "ff-only merge failed (main advanced?)")
        if not self._git("push", "origin", "main").ok:
            self._git("reset", "--hard", "origin/main")
            return PublishOutcome("merge", False, "push failed; reset to origin/main")
        return PublishOutcome("merge", True, "fast-forwarded and pushed origin/main")
