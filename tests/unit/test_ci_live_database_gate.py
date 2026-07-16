from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
RUNNER = ROOT / "scripts" / "ci-live-database-gate.sh"
PINNED_TIMESCALE_IMAGE = (
    "timescale/timescaledb:2.28.2-pg17@"
    "sha256:909e2c7577c074517c8936b6d8799294b33f25fb5bad978a2199250665f1ce1a"
)


def test_ci_runs_the_live_postgres_job_on_existing_events() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    live_job = workflow.split("\n  live-postgres:\n", maxsplit=1)[1]

    assert "pull_request_target" not in workflow
    assert "runs-on: ubuntu-24.04" in live_job
    assert "timeout-minutes: 30" in live_job
    assert "contents: read" in live_job
    assert "persist-credentials: false" in live_job
    assert 'AUTOMATION_ENABLED: "false"' in live_job
    assert 'POLYGON_TOTAL_CALL_BUDGET: "0"' in live_job
    assert "sh scripts/ci-live-database-gate.sh" in live_job
    assert "continue-on-error" not in live_job
    assert "secrets." not in live_job


def test_ci_runner_is_fresh_pinned_and_fail_closed() -> None:
    runner = RUNNER.read_text(encoding="utf-8")

    assert PINNED_TIMESCALE_IMAGE in runner
    assert '"${RUNNER_ENVIRONMENT:-}" = "github-hosted"' in runner
    assert '"${RUNNER_OS:-}" = "Linux"' in runner
    assert '"${RUNNER_ARCH:-}" = "X64"' in runner
    assert '"$workspace/.env"' in runner
    assert '"$workspace/data/pgdata"' in runner
    assert '"$RUNNER_TEMP/stockapi-live-postgres"' in runner
    assert "scripts/db-init,target=/docker-entrypoint-initdb.d,readonly" in runner
    assert "--volume /var/lib/postgresql/data" in runner
    assert "docker rm --force --volumes" in runner
    assert "--publish 127.0.0.1:5432:5432" in runner
    assert "TIMESCALEDB_TELEMETRY=off" in runner
    assert "vendor credentials must be absent" in runner
    assert "export AUTOMATION_ENABLED=false" in runner
    assert "export POLYGON_TOTAL_CALL_BUDGET=0" in runner


def test_ci_runner_opts_in_only_the_postgres_module() -> None:
    runner = RUNNER.read_text(encoding="utf-8")

    assert "stockapi_owner:${owner_password}@127.0.0.1:5432/stockapi_test" in runner
    assert "stockapi_app:${app_password}@127.0.0.1:5432/stockapi_test" in runner
    assert "stockapi_snapshot_builder:${builder_password}@127.0.0.1:5432/stockapi_test" in runner
    assert "TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET=stockapi-ci-container-only" in runner
    assert "stockapi_disposable_live_gate_marker" in runner
    assert 'export TEST_DISPOSABLE_DATABASE_NONCE="$gate_nonce"' in runner
    assert "unset ALEMBIC_CONFIG PYTEST_ADDOPTS PYTEST_PLUGINS" in runner
    assert "uv run --frozen --no-sync pytest" in runner
    assert "tests/integration/test_bars_live_gate.py" in runner
    assert "--tb=short" in runner
    assert "tests/integration \\" not in runner
    assert "-p no:cacheprovider" in runner
