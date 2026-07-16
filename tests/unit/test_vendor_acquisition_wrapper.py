"""Static fail-closed contract for the host acquisition wrapper."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WRAPPER_PATH = REPO_ROOT / "run-vendor-acquisition.ps1"


def _wrapper() -> str:
    return WRAPPER_PATH.read_text(encoding="utf-8")


def test_wrapper_scrubs_and_restores_ambient_secrets_and_routes() -> None:
    wrapper = _wrapper()
    scoped_names = (
        "POLYGON_API_KEY",
        "FMP_API_KEY",
        "FINNHUB_API_KEY",
        "NASDAQ_DATA_LINK_API_KEY",
        "ALPACA_API_KEY",
        "ALPACA_API_SECRET",
        "DATABENTO_API_KEY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "PYTHONHOME",
        "PYTHONPATH",
        "UV_ENV_FILE",
        "UV_PROJECT",
        "UV_WORKING_DIR",
        "UV_PYTHON",
        "UV_NO_SYNC",
        "UV_CONFIG_FILE",
        "UV_PROJECT_ENVIRONMENT",
        "VIRTUAL_ENV",
    )

    for name in scoped_names:
        assert f'"{name}"' in wrapper
    first_scrub = wrapper.index('Remove-Item "Env:$name"')
    first_python = wrapper.index("uv run --no-env-file --frozen --project $PSScriptRoot python")
    assert first_scrub < first_python
    assert '[Environment]::GetEnvironmentVariable($name, "Process")' in wrapper
    assert '[Environment]::SetEnvironmentVariable($name, $priorValue, "Process")' in wrapper
    assert wrapper.rindex("[Environment]::SetEnvironmentVariable") > first_python
    assert wrapper.count("uv run --no-env-file --frozen --project $PSScriptRoot python") == 4
    assert "uv run --frozen" not in wrapper


def test_wrapper_attests_exact_local_timescale_and_migration_head() -> None:
    wrapper = _wrapper()

    for required in (
        '"timescale/timescaledb:2.28.2-pg17"',
        '"/stockapi-timescaledb"',
        "'com.docker.compose.project'",
        "'com.docker.compose.service'",
        '"stock-api"',
        '"timescaledb"',
        '"healthy"',
        '"5432/tcp"',
        '"127.0.0.1"',
        '"0016_selection_policy_fence"',
        "--dbname stockapi_test",
        "\"SELECT current_database() || ''|'' || version_num FROM alembic_version\"",
        '"stockapi_test|$script:expectedMigrationHead"',
    ):
        assert required in wrapper
    assert "Assert-TimescaleContainer -RunningServices $runningServices" in wrapper
    assert "Assert-ExpectedMigrationHead" in wrapper
    assert wrapper.index("Assert-ExpectedMigrationHead\n") < wrapper.index(
        "scripts.vendor_acquisition execute"
    )


def test_wrapper_excludes_every_persistent_actor_before_execute() -> None:
    wrapper = _wrapper()

    for actor in (
        '"api"',
        '"worker"',
        '"beat"',
        '"snapshot-builder"',
        '"stockapi-api"',
        '"stockapi-worker"',
        '"stockapi-beat"',
        '"stockapi-snapshot-builder"',
    ):
        assert actor in wrapper
    assert '$_.CommandLine -match "(?i)\\b(?:celery|uvicorn)\\b"' in wrapper
    final_actor_check = wrapper.rindex(
        "Assert-NoConflictingActors -RunningServices $runningServices"
    )
    assert final_actor_check < wrapper.index("scripts.vendor_acquisition execute")


def test_execute_replans_and_binds_exact_campaign_and_typed_allocation() -> None:
    wrapper = _wrapper()

    for required in (
        'ValidatePattern("^sha256:[0-9a-f]{64}$")',
        "CampaignId",
        "CampaignBudgetDelta",
        "$reviewedPlanOutput.Count -ne 1",
        '$reviewedPlan.status -cne "ready"',
        "$reviewedPlan.plan_id -cne $PlanId",
        '$reviewedPlan.tool_revision -cnotmatch "^[0-9a-f]{40}$"',
        "$reviewedPlan.required_outbound_attempts -ne $MaxCalls",
        "$reviewedPlan.call_allocation.split_page -ne $SplitCalls",
        "$reviewedPlan.call_allocation.dividend_page -ne $DividendCalls",
        "$reviewedPlan.call_allocation.open_close -ne $OpenCloseCalls",
        "$reviewedPlan.campaign_id -cne $CampaignId",
        '$reviewedPlan.campaign_ledger_sha256 -cnotmatch "^sha256:[0-9a-f]{64}$"',
        '$reviewedPlan.global_ledger_sha256 -cnotmatch "^sha256:[0-9a-f]{64}$"',
        "$reviewedPlan.campaign_ledger_record_count -gt",
        "$reviewedPlan.global_ledger_record_count",
        "$reviewedPlan.campaign_required_budget_delta -ne $CampaignBudgetDelta",
        "$reviewedPlan.max_recovery_calls -ne $maxRecoveryCalls",
        "$reviewedPlan.campaign_hard_max_authorized_calls -gt $maxCampaignCalls",
        "--campaign-id $CampaignId",
        "--campaign-budget-delta $CampaignBudgetDelta",
    ):
        assert required in wrapper
    replan = wrapper.index("$reviewedPlanOutput = @(")
    execute = wrapper.index("scripts.vendor_acquisition execute")
    assert replan < execute


def test_execute_imports_only_detached_reviewed_code_with_primary_ledgers() -> None:
    wrapper = _wrapper()

    for required in (
        "git worktree add --detach $safeBuildContext $reviewedPlan.tool_revision",
        "$detachedRevision -cne $reviewedPlan.tool_revision",
        "$detachedStatus.Count -ne 0",
        "$detachedFinalRevision -cne $reviewedPlan.tool_revision",
        "$detachedFinalStatus.Count -ne 0",
        "Assert-PrimaryGitState -ExpectedRevision $reviewedPlan.tool_revision",
        "$env:PYTHONPATH = $safeBuildContext",
        (
            "uv run --no-env-file --frozen --project $PSScriptRoot "
            "python -P -m scripts.vendor_acquisition execute"
        ),
        '"vendor_acquisition_attempts.jsonl"',
        '"vendor_backfill_attempts.jsonl"',
        "--ledger-path $acquisitionLedgerPath",
        "--legacy-ledger-path $legacyLedgerPath",
        "git worktree remove --force $safeBuildContext",
    ):
        assert required in wrapper
    assert (
        wrapper.count("Assert-PrimaryGitState -ExpectedRevision $reviewedPlan.tool_revision") == 2
    )
    final_git_check = wrapper.rindex(
        "Assert-PrimaryGitState -ExpectedRevision $reviewedPlan.tool_revision"
    )
    detached_execute = wrapper.index(
        "uv run --no-env-file --frozen --project $PSScriptRoot "
        "python -P -m scripts.vendor_acquisition execute"
    )
    assert final_git_check < detached_execute


def test_wrapper_rejects_reparse_worktree_paths_before_use_and_removal() -> None:
    wrapper = _wrapper()

    assert "function Assert-NonReparseDirectory" in wrapper
    assert "[System.IO.FileAttributes]::ReparsePoint" in wrapper
    assert wrapper.count('-Description "temporary acquisition worktree root"') == 2
    assert wrapper.count('-Description "temporary acquisition worktree"') == 3
    worktree_add = wrapper.index("git worktree add --detach")
    first_context_check = wrapper.index(
        '-Description "temporary acquisition worktree"', worktree_add
    )
    first_detached_git = wrapper.index("git -C $safeBuildContext", worktree_add)
    assert worktree_add < first_context_check < first_detached_git
    cleanup_check = wrapper.rindex('-Description "temporary acquisition worktree"')
    cleanup_remove = wrapper.index("git worktree remove --force")
    assert cleanup_check < cleanup_remove


def test_wrapper_proves_primary_tree_before_loading_project_code() -> None:
    wrapper = _wrapper()

    first_project_code = wrapper.index(
        "uv run --no-env-file --frozen --project $PSScriptRoot python"
    )
    initial_revision = wrapper.index("$primaryRevision = ([string](git rev-parse HEAD)).Trim()")
    initial_integrity_check = wrapper.index(
        "Assert-PrimaryGitState -ExpectedRevision $primaryRevision"
    )
    assert initial_revision < initial_integrity_check < first_project_code
