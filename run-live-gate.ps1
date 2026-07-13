# One-shot live-DB gate runner. This script is intentionally hard-bound to the
# owner-designated local throwaway database and never performs vendor calls.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath ".env")) {
    throw ".env is required"
}
$vars = @{}
Get-Content -LiteralPath ".env" |
    Where-Object { $_ -match "^[A-Z_][A-Z0-9_]*=" } |
    ForEach-Object {
        $key, $value = $_ -split "=", 2
        $vars[$key] = $value
    }

$required = @(
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_APP_PASSWORD",
    "POSTGRES_SNAPSHOT_BUILDER_PASSWORD"
)
foreach ($name in $required) {
    if (-not $vars.ContainsKey($name) -or [string]::IsNullOrWhiteSpace($vars[$name])) {
        throw "$name must be non-empty in .env"
    }
}
if ($vars.POSTGRES_DB -cne "stockapi_test") {
    throw "refusing destructive gate: POSTGRES_DB must be exactly stockapi_test"
}
if ($vars.POSTGRES_USER -cne "stockapi_owner") {
    throw "refusing destructive gate: POSTGRES_USER must be exactly stockapi_owner"
}
$secrets = @(
    $vars.POSTGRES_PASSWORD,
    $vars.POSTGRES_APP_PASSWORD,
    $vars.POSTGRES_SNAPSHOT_BUILDER_PASSWORD
)
if (($secrets | Select-Object -Unique).Count -ne $secrets.Count) {
    throw "owner, runtime, and snapshot-builder passwords must be distinct"
}

docker compose up -d timescaledb
if ($LASTEXITCODE -ne 0) {
    throw "docker compose failed to start timescaledb"
}

Write-Host "waiting for timescaledb health (maximum 5 minutes)..."
$deadline = (Get-Date).AddMinutes(5)
do {
    if ((Get-Date) -ge $deadline) {
        docker compose logs --tail 100 timescaledb
        throw "timed out waiting for timescaledb health"
    }
    Start-Sleep -Seconds 3
    $health = docker inspect --format "{{.State.Health.Status}}" stockapi-timescaledb 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "stockapi-timescaledb is missing or stopped"
    }
    if ($health -eq "unhealthy") {
        docker compose logs --tail 100 timescaledb
        throw "timescaledb became unhealthy"
    }
} while ($health -ne "healthy")

$ownerPassword = [uri]::EscapeDataString($vars.POSTGRES_PASSWORD)
$runtimePassword = [uri]::EscapeDataString($vars.POSTGRES_APP_PASSWORD)
$builderPassword = [uri]::EscapeDataString($vars.POSTGRES_SNAPSHOT_BUILDER_PASSWORD)
$database = $vars.POSTGRES_DB

try {
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://stockapi_owner:$ownerPassword@localhost:5432/$database"
    $env:TEST_RUNTIME_DATABASE_URL = "postgresql+asyncpg://stockapi_app:$runtimePassword@localhost:5432/$database"
    $env:TEST_SNAPSHOT_BUILDER_DATABASE_URL = "postgresql+asyncpg://stockapi_snapshot_builder:$builderPassword@localhost:5432/$database"
    $env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET = "stockapi-test-only"
    uv run pytest tests/integration -v
    if ($LASTEXITCODE -ne 0) {
        throw "live integration gate failed"
    }
}
finally {
    Remove-Item Env:TEST_DATABASE_URL -ErrorAction SilentlyContinue
    Remove-Item Env:TEST_RUNTIME_DATABASE_URL -ErrorAction SilentlyContinue
    Remove-Item Env:TEST_SNAPSHOT_BUILDER_DATABASE_URL -ErrorAction SilentlyContinue
    Remove-Item Env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET -ErrorAction SilentlyContinue
}
