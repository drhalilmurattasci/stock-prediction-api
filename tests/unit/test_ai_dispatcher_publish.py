"""Publish-decision, authorization-store (strict JSON), and merge-count tests."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from ai_dispatcher import publish
from ai_dispatcher.config import (
    DEFAULT_HIGH_RISK_GLOBS,
    DEFAULT_NEVER_AUTOMERGE_GLOBS,
    default_config,
)
from ai_dispatcher.scope_guard import matches_any

TODAY = date(2026, 7, 9)


def _auth(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": "docs-auth",
        "scope": ["docs/**", "*.md"],
        "max_merges": 5,
        "expires": "2026-12-31",
        "granted_by": "me",
    }
    base.update(over)
    return base


def _ledger(*entries: dict[str, Any]) -> str:
    return json.dumps({"authorizations": list(entries)})


LEDGER = _ledger(
    _auth(),  # docs-auth (valid)
    _auth(id="expired-auth", scope=["app/**"], expires="2020-01-01"),  # expired -> dropped
)


def test_classify_risk() -> None:
    assert publish.classify_risk(["app/main.py"], DEFAULT_HIGH_RISK_GLOBS) == "high"
    assert publish.classify_risk(["docs/x.md"], DEFAULT_HIGH_RISK_GLOBS) == "low"


def test_parse_drops_expired_and_keeps_valid() -> None:
    auths = publish.parse_authorizations(LEDGER, today=TODAY)
    assert [a.auth_id for a in auths] == ["docs-auth"]
    assert auths[0].scope_globs == ("docs/**", "*.md")
    assert auths[0].max_merges == 5
    assert auths[0].expires == date(2026, 12, 31)


@pytest.mark.parametrize(
    "bad",
    [
        _auth(scope="docs/**"),  # scope not a list
        _auth(scope=[]),  # empty scope
        _auth(scope=["docs/**", 5]),  # non-str glob
        _auth(scope=["a, b"]),  # comma delimiter smuggling
        _auth(scope=["a b"]),  # whitespace in glob
        _auth(scope=["a\u00a0b"]),  # non-ASCII whitespace in glob
        _auth(scope=["`x`"]),  # backtick smuggling
        _auth(max_merges=0),  # non-positive
        _auth(max_merges=-1),
        _auth(max_merges=True),  # bool is not a valid int cap
        _auth(max_merges="5"),  # str, not int
        _auth(expires="not-a-date"),  # bad date value
        _auth(expires=20260101),  # non-str
        _auth(id="bad id"),  # invalid id token
        {
            "id": "x",
            "scope": ["docs/**"],
            "max_merges": 5,
            "expires": "2026-12-31",
        },  # missing granted_by
        {**_auth(), "extra": 1},  # unknown extra key
    ],
)
def test_parse_fails_closed_on_malformed_entry(bad: dict[str, Any]) -> None:
    with pytest.warns(RuntimeWarning, match="invalid entry"):
        assert publish.parse_authorizations(_ledger(bad), today=TODAY) == []


def test_parse_fails_closed_for_whole_store_when_any_entry_is_malformed() -> None:
    with pytest.warns(RuntimeWarning, match="invalid entry"):
        assert (
            publish.parse_authorizations(_ledger(_auth(), _auth(scope=["bad scope"])), today=TODAY)
            == []
        )


def test_missing_expires_is_not_never_expires() -> None:
    # The markdown bug: a missing/mistyped EXPIRES defaulted to None -> "never
    # expires". In JSON, expires is mandatory, so its absence drops the entry.
    entry = {"id": "x", "scope": ["app/**"], "max_merges": 5, "granted_by": "me"}
    with pytest.warns(RuntimeWarning, match="invalid entry"):
        assert publish.parse_authorizations(_ledger(entry), today=TODAY) == []


def test_duplicate_json_key_fails_closed() -> None:
    # Two "scope" keys must not last-wins-broaden to '**'.
    raw = (
        '{"authorizations":[{"id":"x","scope":["docs/**"],"scope":["**"],'
        '"max_merges":1,"expires":"2026-12-31","granted_by":"me"}]}'
    )
    with pytest.warns(RuntimeWarning):
        assert publish.parse_authorizations(raw, today=TODAY) == []


def test_invalid_json_and_wrong_shape_fail_closed() -> None:
    with pytest.warns(RuntimeWarning):
        assert publish.parse_authorizations("not json {", today=TODAY) == []
    assert publish.parse_authorizations("[]", today=TODAY) == []  # not an object
    assert publish.parse_authorizations('{"authorizations": {}}', today=TODAY) == []  # not a list
    assert (
        publish.parse_authorizations(
            json.dumps({"authorizations": [_auth()], "comment": "extra root key"}), today=TODAY
        )
        == []
    )


def test_empty_store_has_no_authorizations() -> None:
    assert publish.parse_authorizations('{"authorizations": []}', today=TODAY) == []


def test_expiry_filter_applies_even_without_explicit_today() -> None:
    # A caller that forgets ``today`` must NOT resurrect an expired grant; the
    # filter defaults to the real today (2000 is always in the past).
    entry = _auth(id="past", expires="2000-01-01")
    assert publish.parse_authorizations(_ledger(entry)) == []


@pytest.mark.parametrize("bad_date", ["20261231", "2026-W53-4", "2026-12-31T00:00:00"])
def test_expires_must_be_yyyy_mm_dd(bad_date: str) -> None:
    with pytest.warns(RuntimeWarning, match="invalid entry"):
        assert publish.parse_authorizations(_ledger(_auth(expires=bad_date)), today=TODAY) == []


@pytest.mark.parametrize("bad_scope", [["docs**"], ["**docs"], ["a/**b"], ["x**/y"]])
def test_scope_double_star_must_occupy_a_full_segment(bad_scope: list[str]) -> None:
    with pytest.warns(RuntimeWarning, match="invalid entry"):
        assert publish.parse_authorizations(_ledger(_auth(scope=bad_scope)), today=TODAY) == []


def test_scope_double_star_full_segment_forms_are_ok() -> None:
    auths = publish.parse_authorizations(
        _ledger(_auth(scope=["docs/**", "**/x", "**"])), today=TODAY
    )
    assert len(auths) == 1


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


def test_step6_doc_smoke_authorization_acceptance_shape() -> None:
    auth_id = "step6-doc-smoke"
    changed = ["docs/step6_smoke.md"]
    auths = publish.parse_authorizations(
        _ledger(
            _auth(
                id=auth_id,
                scope=["docs/step6_smoke.md"],
                max_merges=1,
                expires="2026-07-17",
                granted_by="drhalilmurattasci",
            )
        ),
        today=date(2026, 7, 10),
    )

    assert len(auths) == 1
    assert auths[0].auth_id == auth_id
    assert auths[0].scope_globs == ("docs/step6_smoke.md",)
    assert auths[0].max_merges == 1
    assert auths[0].expires == date(2026, 7, 17)
    assert auths[0].granted_by == "drhalilmurattasci"
    assert publish.classify_risk(changed, DEFAULT_HIGH_RISK_GLOBS) == "low"
    assert not matches_any(changed[0], DEFAULT_NEVER_AUTOMERGE_GLOBS)

    fresh = publish.decide_publish(
        "main",
        changed_files=changed,
        authorizations=auths,
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=DEFAULT_NEVER_AUTOMERGE_GLOBS,
    )
    assert fresh.action == "merge"
    assert fresh.auth_id == auth_id

    exhausted = publish.decide_publish(
        "main",
        changed_files=changed,
        authorizations=auths,
        used_counts={auth_id: 1},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=DEFAULT_NEVER_AUTOMERGE_GLOBS,
    )
    assert exhausted.action == "pr"

    sibling = publish.decide_publish(
        "main",
        changed_files=["docs/other.md"],
        authorizations=auths,
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=DEFAULT_NEVER_AUTOMERGE_GLOBS,
    )
    assert sibling.action == "pr"

    partial = publish.decide_publish(
        "main",
        changed_files=[*changed, "docs/other.md"],
        authorizations=auths,
        used_counts={},
        high_risk_globs=DEFAULT_HIGH_RISK_GLOBS,
        never_automerge_globs=DEFAULT_NEVER_AUTOMERGE_GLOBS,
    )
    assert partial.action == "pr"


def test_merge_counts_roundtrip(tmp_path: Path) -> None:
    config = default_config(tmp_path)
    assert publish.merge_counts(config) == {}
    publish.record_merge(config, auth_id="docs-auth", dispatch_id="d1", sha="abc")
    publish.record_merge(config, auth_id="docs-auth", dispatch_id="d2", sha="def")
    assert publish.merge_counts(config) == {"docs-auth": 2}


def test_never_automerge_forces_pr_even_with_covering_auth() -> None:
    auths = publish.parse_authorizations(
        _ledger(_auth(id="self-auth", scope=["ai_dispatcher/**"])), today=TODAY
    )
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

    def runner(argv: Any, **_: Any) -> CommandResult:
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


def test_publisher_fails_closed_on_unexpected_action(tmp_path: Path) -> None:
    # H1: _merge_to_main (the only path that pushes origin/main) must be reached
    # ONLY by an explicit "merge" action. An unexpected/future action must not
    # fall through to a push — it fails closed with no remote effect.
    from ai_dispatcher.subprocess_utils import CommandResult

    calls: list[tuple[str, ...]] = []

    def runner(argv: Any, **_: Any) -> CommandResult:
        argv_t = tuple(argv)
        calls.append(argv_t)
        stdout = "docs/a.md\n" if ("diff" in argv_t and "--cached" in argv_t) else ""
        return CommandResult(argv=argv_t, exit_code=0, stdout=stdout, stderr="", duration_s=0.0)

    def run(action: str) -> publish.PublishOutcome:
        calls.clear()
        publisher = publish.Publisher(default_config(tmp_path), runner=runner)
        return publisher.publish(
            publish.PublishDecision(action, "reason", "docs-auth"),  # type: ignore[arg-type]
            branch="ai-dispatch/demo",
            commit_message="msg",
            title="t",
            body="b",
            changed_files=["docs/a.md"],
        )

    push_main = ("git", "push", "origin", "main")

    # Positive control: a genuine "merge" decision DOES reach the push-to-main path.
    merged = run("merge")
    assert merged.ok
    assert push_main in calls

    # H1: an unrecognized action must NOT push to main and must fail closed.
    bogus = run("release")  # a hypothetical future/typo'd action publish() doesn't handle
    assert not bogus.ok
    assert "unknown publish action" in bogus.detail
    assert push_main not in calls
    assert not any("--ff-only" in c for c in calls)
