"""Static configuration for the ai_dispatcher dispatch loop.

Everything the loop needs to locate state, bound its work, and decide what is
safe to auto-publish lives here as a frozen dataclass so a run is fully
reproducible from ``(repo_root, overrides)``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

# --- exit-code taxonomy (mirrors the RGE loop's contract) -------------------
#: A model/verify subprocess that exceeded its wall-clock budget.
EXIT_TIMEOUT = 124
#: A model subprocess whose output stopped growing for the stall window.
EXIT_STALL = 125

# --- verify gate ------------------------------------------------------------
#: Verify steps, in order, mirroring the ``lint-type-test`` job of
#: ``.github/workflows/ci.yml`` one-for-one. A dispatch that passes all of
#: these means "CI would pass". The step *count* is load-bearing: the publisher
#: refuses a run unless every step ran (see ``verify.py``), so a trimmed gate
#: cannot self-certify — the hole RGE's audit flagged.
DEFAULT_VERIFY_STEPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ruff check", ("uv", "run", "ruff", "check", ".")),
    ("ruff format --check", ("uv", "run", "ruff", "format", "--check", ".")),
    ("mypy", ("uv", "run", "mypy")),
    ("pytest", ("uv", "run", "pytest")),
)

# --- risk tiers -------------------------------------------------------------
#: Path globs whose modification must NOT auto-merge to ``main``. A change
#: touching any of these is downgraded to a PR for human review (fail-closed).
#: The dispatcher's own package is included so it can never auto-merge edits to
#: itself, and the verify script is implicitly covered by ``ai_dispatcher/**``.
DEFAULT_HIGH_RISK_GLOBS: tuple[str, ...] = (
    "app/**",
    "data_sources/**",
    "ingestion/**",
    "ml/**",
    "migrations/**",
    "ai_dispatcher/**",
    "pyproject.toml",
    "uv.lock",
    ".github/**",
    "docker-compose.yml",
    "Dockerfile",
    "alembic.ini",
)

#: Path globs that may NEVER auto-merge to ``main``, even with a covering
#: authorization — a change touching any of these always downgrades to a PR.
#: The dispatcher's own package and CI are here: auto-merging edits to the
#: safety machinery (or the pipeline that verifies it) is a footgun, so those
#: always get a human at the merge.
DEFAULT_NEVER_AUTOMERGE_GLOBS: tuple[str, ...] = (
    "ai_dispatcher/**",
    ".github/**",
)


@dataclass(frozen=True)
class DispatcherConfig:
    """Immutable configuration for one dispatcher installation."""

    repo_root: Path

    # tool invocations (front of the argv; the rest is appended per call)
    git: tuple[str, ...] = ("git",)
    gh: tuple[str, ...] = ("gh",)
    claude: tuple[str, ...] = ("claude",)
    codex: tuple[str, ...] = ("codex",)

    # bounds
    max_plan_revisions: int = 2
    max_correction_rounds: int = 1
    max_consecutive_failures: int = 3
    seatbelt_interval: int = 10

    # timeouts (seconds)
    model_timeout_s: int = 1800
    verify_timeout_s: int = 3600
    stall_threshold_s: int = 0  # 0 = disabled

    verify_steps: tuple[tuple[str, tuple[str, ...]], ...] = DEFAULT_VERIFY_STEPS
    high_risk_globs: tuple[str, ...] = DEFAULT_HIGH_RISK_GLOBS
    never_automerge_globs: tuple[str, ...] = DEFAULT_NEVER_AUTOMERGE_GLOBS
    #: Dispatcher-generated audit artifacts (handoff packets). These are
    #: gitignored so they normally never enter the change-set, but they are also
    #: carved out of the publish decision (not reviewable "product" changes) so a
    #: stray artifact can never block or mis-authorize an auto-merge.
    artifact_globs: tuple[str, ...] = ("ai_dispatcher/handoffs/**",)

    # models (None = CLI default)
    claude_model: str | None = None
    codex_model: str | None = None

    # integration branch the loop must be synced with before starting
    integration_ref: str = "origin/main"

    # relative locations under repo_root (kept as names so they stay portable)
    ai_dirname: str = ".ai_dispatch"
    handoffs_dirname: str = "ai_dispatcher/handoffs"

    def with_overrides(self, **changes: object) -> DispatcherConfig:
        """Return a copy with the given fields replaced."""
        return replace(self, **changes)  # type: ignore[arg-type]

    # --- derived paths ------------------------------------------------------
    @property
    def ai_dir(self) -> Path:
        return self.repo_root / self.ai_dirname

    @property
    def handoffs_dir(self) -> Path:
        return self.repo_root / self.handoffs_dirname

    @property
    def control_schema_path(self) -> Path:
        return self.repo_root / "ai_dispatcher" / "schemas" / "codex_control.schema.json"

    @property
    def tasks_file(self) -> Path:
        return self.repo_root / "ai_dispatcher" / "dispatch.tasks.md"

    @property
    def authorizations_file(self) -> Path:
        return self.repo_root / "ai_dispatcher" / "AUTHORIZATIONS.md"

    @property
    def halt_sentinel(self) -> Path:
        return self.ai_dir / "dispatch.auto-halt"

    @property
    def stop_sentinel(self) -> Path:
        return self.ai_dir / "dispatch.guard-stop"

    @property
    def failure_counter(self) -> Path:
        return self.ai_dir / "dispatch.consecutive-failures.json"

    @property
    def seatbelt_counter(self) -> Path:
        return self.ai_dir / "dispatch.seatbelt.json"

    def run_dir(self, dispatch_id: str) -> Path:
        return self.ai_dir / f"dispatch-{dispatch_id}"


def default_config(repo_root: Path, **overrides: object) -> DispatcherConfig:
    """Build a config rooted at ``repo_root`` with optional field overrides."""
    cfg = DispatcherConfig(repo_root=repo_root.resolve())
    return cfg.with_overrides(**overrides) if overrides else cfg
