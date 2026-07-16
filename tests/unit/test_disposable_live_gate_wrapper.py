"""Static fail-closed contract for the local disposable live-DB runner."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "run-disposable-live-gate.ps1"
LEGACY_WRAPPER = ROOT / "run-live-gate.ps1"
LIVE_GATE = ROOT / "tests" / "integration" / "test_bars_live_gate.py"
PINNED_TIMESCALE_IMAGE = (
    "timescale/timescaledb:2.28.2-pg17@"
    "sha256:909e2c7577c074517c8936b6d8799294b33f25fb5bad978a2199250665f1ce1a"
)


def _wrapper() -> str:
    return WRAPPER.read_text(encoding="utf-8")


def test_runner_uses_an_isolated_digest_pinned_container() -> None:
    wrapper = _wrapper()

    assert PINNED_TIMESCALE_IMAGE in wrapper
    assert '$script:dockerArgs = @("--context", "desktop-linux")' in wrapper
    assert '"npipe:////./pipe/dockerDesktopLinuxEngine"' in wrapper
    assert "docker @script:dockerArgs run --detach" in wrapper
    assert '--publish "127.0.0.1::5432"' in wrapper
    assert "--volume /var/lib/postgresql/data" in wrapper
    assert (
        '--mount "type=bind,source=$initDirectory,target=/docker-entrypoint-initdb.d,readonly"'
    ) in wrapper
    assert " docker compose " not in f" {wrapper.lower()} "
    assert "docker-compose" not in wrapper.lower()
    assert "data/pgdata" not in wrapper


def test_runner_refuses_port_5432_and_attests_the_random_binding() -> None:
    wrapper = _wrapper()

    assert "docker @script:dockerArgs port $containerId 5432/tcp" in wrapper
    assert '"^127\\.0\\.0\\.1:(?<port>[0-9]+)$"' in wrapper
    assert "$hostPort -lt 1024" in wrapper
    assert "$hostPort -gt 65535" in wrapper
    assert "$hostPort -eq 5432" in wrapper
    assert '[string]$portBindings[0].HostIp -cne "127.0.0.1"' in wrapper
    assert "[string]$portBindings[0].HostPort -cne [string]$hostPort" in wrapper


def test_runner_generates_distinct_credentials_and_a_nonce_marker() -> None:
    wrapper = _wrapper()

    assert "Global\\StockApiMutatingOperator" in wrapper
    assert wrapper.count("New-RandomHex -ByteCount 24") == 3
    assert "Select-Object -Unique" in wrapper
    assert "stockapi.live-gate.nonce=$nonce" in wrapper
    assert "CREATE TABLE public.stockapi_disposable_live_gate_marker" in wrapper
    assert "VALUES (true, '$nonce')" in wrapper
    assert "Assert-DisposableMarker -Identity $containerId -ExpectedNonce $nonce" in wrapper
    assert (
        wrapper.count("Assert-DisposableMarker -Identity $containerId -ExpectedNonce $nonce") == 2
    )


def test_runner_replaces_every_database_route_and_closes_vendor_io() -> None:
    wrapper = _wrapper()

    for name in (
        "TEST_DATABASE_URL",
        "TEST_RUNTIME_DATABASE_URL",
        "TEST_SNAPSHOT_BUILDER_DATABASE_URL",
        "DATABASE_URL",
        "MIGRATION_DATABASE_URL",
    ):
        assert f'-Name "{name}"' in wrapper
    assert '"stockapi-disposable-container-only"' in wrapper
    assert '-Name "TEST_DISPOSABLE_DATABASE_HOST_PORT"' in wrapper
    assert '-Name "TEST_DISPOSABLE_DATABASE_NONCE"' in wrapper
    assert '-Name "AUTOMATION_ENABLED" -Value "false"' in wrapper
    assert '-Name "POLYGON_TOTAL_CALL_BUDGET" -Value "0"' in wrapper
    for name in (
        "POLYGON_API_KEY",
        "FMP_API_KEY",
        "FINNHUB_API_KEY",
        "NASDAQ_DATA_LINK_API_KEY",
        "ALPACA_API_KEY",
        "ALPACA_API_SECRET",
        "DATABENTO_API_KEY",
    ):
        assert f'"{name}"' in wrapper
    assert 'Set-ScopedEnvironment -Name $name -Value ""' in wrapper
    assert "POLYGON_TOTAL_CALL_BUDGET=1" not in wrapper


def test_runner_rejects_ambient_test_and_migration_redirection() -> None:
    wrapper = _wrapper()

    for name in ("ALEMBIC_CONFIG", "PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        assert f'"{name}"' in wrapper
    assert 'throw "$name must be unset for the disposable live-database gate"' in wrapper


def test_runner_executes_only_the_live_integration_module() -> None:
    wrapper = _wrapper()

    command = "uv run --no-env-file --frozen --no-sync --project $PSScriptRoot pytest"
    assert wrapper.count(command) == 1
    assert "tests/integration/test_bars_live_gate.py" in wrapper
    assert "--tb=short" in wrapper
    assert "tests/integration `" not in wrapper
    assert "-p no:cacheprovider" in wrapper
    assert '--basetemp "data/pytest-disposable-live-$nonce"' in wrapper


def test_cleanup_is_identity_bound_and_removes_only_the_anonymous_volume() -> None:
    wrapper = _wrapper()

    assert "Assert-OwnedContainer" in wrapper
    assert "-ExpectedId $containerId" in wrapper
    assert "-ExpectedNonce $nonce" in wrapper
    assert "docker @script:dockerArgs rm --force --volumes $containerId" in wrapper
    assert wrapper.count("docker @script:dockerArgs rm --force --volumes") == 1
    assert "docker @script:dockerArgs volume inspect $volumeName" in wrapper
    assert "volume rm" not in wrapper.lower()
    assert "volume prune" not in wrapper.lower()
    assert "system prune" not in wrapper.lower()
    assert "rm --force --volumes $containerName" not in wrapper


def test_diagnostic_logs_cannot_interrupt_identity_bound_cleanup() -> None:
    wrapper = _wrapper()

    helper = wrapper.index("function Write-DockerLogsBestEffort")
    relaxed = wrapper.index('$ErrorActionPreference = "Continue"', helper)
    logs = wrapper.index("docker @script:dockerArgs logs --tail $Tail $Identity 2>&1", relaxed)
    restored = wrapper.index("$ErrorActionPreference = $priorErrorActionPreference", logs)
    cleanup_diagnostics = wrapper.index(
        "Write-DockerLogsBestEffort -Identity $containerId -Tail 200",
        restored,
    )
    removal = wrapper.index(
        "docker @script:dockerArgs rm --force --volumes $containerId",
        cleanup_diagnostics,
    )

    assert helper < relaxed < logs < restored < cleanup_diagnostics < removal
    assert wrapper.count("docker @script:dockerArgs logs") == 1


def test_runner_compares_port_5432_state_and_restores_the_environment() -> None:
    wrapper = _wrapper()

    before = wrapper.index("$persistentSnapshotBefore = Get-HostPort5432Snapshot")
    create = wrapper.index("docker @script:dockerArgs run --detach")
    cleanup = wrapper.index("docker @script:dockerArgs rm --force --volumes $containerId")
    after = wrapper.index("$persistentSnapshotAfter = Get-HostPort5432Snapshot")
    compare = wrapper.index("$persistentSnapshotAfter -cne $persistentSnapshotBefore")
    restore = wrapper.index("$priorEnvironment[$name]", compare)
    release = wrapper.index("$mutex.ReleaseMutex()", restore)

    assert before < create < cleanup < after < compare < restore < release
    assert 'HostPort -ceq "5432"' in wrapper
    assert "StartedAt = [string]$inspection.State.StartedAt" in wrapper
    assert '[Environment]::GetEnvironmentVariable($name, "Process")' in wrapper
    assert "[Environment]::SetEnvironmentVariable(" in wrapper


def test_integration_reset_accepts_only_the_exact_disposable_marker_path() -> None:
    gate = LIVE_GATE.read_text(encoding="utf-8")

    assert '"stockapi-disposable-container-only"' in gate
    assert '"stockapi-ci-container-only"' in gate
    assert '"stockapi-test-only"' not in gate
    assert 'os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"' in gate
    assert 'os.environ.get("TEST_DISPOSABLE_DATABASE_HOST_PORT", "")' in gate
    assert "parsed.port is None" in gate
    assert "disposable_port == 5432" in gate
    assert 'os.environ.get("TEST_DISPOSABLE_DATABASE_NONCE", "")' in gate
    assert "stockapi_disposable_live_gate_marker" in gate
    verify = gate.index("_verify_throwaway_database_marker(url)")
    destructive_drop = gate.index("_drop_project_schema(url)", verify)
    assert verify < destructive_drop


def test_legacy_persistent_runner_refuses_before_any_docker_action() -> None:
    legacy = LEGACY_WRAPPER.read_text(encoding="utf-8")

    refusal = legacy.index(
        'throw "the persistent port-5432 live gate is retired; use .\\run-disposable-live-gate.ps1"'
    )
    first_docker = legacy.index("docker ", refusal)
    assert refusal < first_docker
