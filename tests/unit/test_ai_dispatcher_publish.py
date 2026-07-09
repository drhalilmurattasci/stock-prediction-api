"""Publish-decision, authorization-ledger, and merge-count tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_dispatcher import publish
from ai_dispatcher.config import DEFAULT_HIGH_RISK_GLOBS, default_config

LEDGER = """# ledger

## AUTH docs-auth

- SCOPE: `docs/**`, `*.md`
- MAX_MERGES: 5
- EXPIRES: 2026-12-31
- GRANTED_BY: me

## AUTH expired-auth

- SCOPE: `app/**`
- MAX_MERGES: 5
- EXPIRES: 2020-01-01
- GRANTED_BY: me

## AUTH no-scope-auth

- MAX_MERGES: 5
- EXPIRES: 2026-12-31
"""

TODAY = date(2026, 7, 9)


def test_classify_risk() -> None:
    assert publish.classify_risk(["app/main.py"], DEFAULT_HIGH_RISK_GLOBS) == "high"
    assert publish.classify_risk(["docs/x.md"], DEFAULT_HIGH_RISK_GLOBS) == "low"


def test_parse_authorizations_drops_expired_and_invalid() -> None:
    auths = publish.parse_authorizations(LEDGER, today=TODAY)
    assert [a.auth_id for a in auths] == ["docs-auth"]
    assert auths[0].scope_globs == ("docs/**", "*.md")
    assert auths[0].max_merges == 5


def test_decide_publish_branch_and_pr_modes() -> None:
    assert (
        publish.decide_publish(
            "branch",
            changed_files=["docs/a.md"],
            authorizations=[],
            used_counts={},
            high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        ).action
        == "branch"
    )
    assert (
        publish.decide_publish(
            "pr",
            changed_files=["docs/a.md"],
            authorizations=[],
            used_counts={},
            high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        ).action
        == "pr"
    )


def test_decide_publish_main_merges_only_when_fully_covered() -> None:
    auths = publish.parse_authorizations(LEDGER, today=TODAY)

    covered = publish.decide_publish(
        "main",
        changed_files=["docs/a.md", "README.md"],
        authorizations=auths,
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
    )
    assert covered.action == "merge"
    assert covered.auth_id == "docs-auth"

    uncovered = publish.decide_publish(
        "main",
        changed_files=["app/x.py"],
        authorizations=auths,
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
    )
    assert uncovered.action == "pr"

    exhausted = publish.decide_publish(
        "main",
        changed_files=["docs/a.md"],
        authorizations=auths,
        used_counts={"docs-auth": 5},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
    )
    assert exhausted.action == "pr"


def test_merge_counts_roundtrip(tmp_path: Path) -> None:
    config = default_config(tmp_path)
    assert publish.merge_counts(config) == {}
    publish.record_merge(config, auth_id="docs-auth", dispatch_id="d1", sha="abc")
    publish.record_merge(config, auth_id="docs-auth", dispatch_id="d2", sha="def")
    assert publish.merge_counts(config) == {"docs-auth": 2}


NEVER_LEDGER = """
## AUTH self-auth

- SCOPE: `ai_dispatcher/**`
- MAX_MERGES: 5
- EXPIRES: 2026-12-31
- GRANTED_BY: me
"""


def test_never_automerge_forces_pr_even_with_covering_auth() -> None:
    auths = publish.parse_authorizations(NEVER_LEDGER, today=TODAY)
    decision = publish.decide_publish(
        "main",
        changed_files=["ai_dispatcher/loop.py"],
        authorizations=auths,  # explicitly covers ai_dispatcher/**
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=["ai_dispatcher/**", ".github/**"],
    )
    assert decision.action == "pr"
    assert "protected" in decision.reason


def test_dispatcher_artifacts_do_not_block_or_need_auth_for_automerge() -> None:
    # A docs task's change-set also carries auto-generated handoff packets under
    # ai_dispatcher/handoffs/. Those must NOT trip the ai_dispatcher/** protected
    # guard nor require the authorization to name them.
    auths = publish.parse_authorizations(LEDGER, today=TODAY)  # docs-auth covers docs/**, *.md
    decision = publish.decide_publish(
        "main",
        changed_files=["docs/x.md", "ai_dispatcher/handoffs/T1_TASK_x.md"],
        authorizations=auths,
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=["ai_dispatcher/**", ".github/**"],
        artifact_globs=["ai_dispatcher/handoffs/**"],
    )
    assert decision.action == "merge"
    assert decision.auth_id == "docs-auth"


def test_only_dispatcher_artifacts_changed_is_a_noop() -> None:
    decision = publish.decide_publish(
        "main",
        changed_files=["ai_dispatcher/handoffs/T1_TASK_x.md"],
        authorizations=[],
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=["ai_dispatcher/**"],
        artifact_globs=["ai_dispatcher/handoffs/**"],
    )
    assert decision.action == "branch"


def test_publisher_refuses_to_commit_unauthorized_files(tmp_path: Path) -> None:
    from ai_dispatcher.subprocess_utils import CommandResult

    def runner(argv, **_):  # type: ignore[no-untyped-def]
        argv_t = tuple(argv)
        stdout = ""
        if "diff" in argv_t and "--cached" in argv_t:
            # git staged MORE than the authorized set (a stray file slipped in)
            stdout = "docs/a.md\napp/evil.py\n"
        return CommandResult(argv=argv_t, exit_code=0, stdout=stdout, stderr="", duration_s=0.0)

    publisher = publish.Publisher(default_config(tmp_path), runner=runner)
    outcome = publisher.publish(
        publish.PublishDecision("merge", "authorized by x", "x"),
        branch="ai-dispatch/demo",
        commit_message="msg",
        title="t",
        body="b",
        changed_files=["docs/a.md"],
    )
    assert not outcome.ok
    assert "unauthorized" in outcome.detail
