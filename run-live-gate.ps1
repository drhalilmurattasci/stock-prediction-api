# One-shot live-DB gate runner. This script is intentionally hard-bound to the
# owner-designated local throwaway database and never performs vendor calls.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiMutatingOperator")
$mutexHeld = $false
$testVariableNames = @(
    "TEST_DATABASE_URL",
    "TEST_RUNTIME_DATABASE_URL",
    "TEST_SNAPSHOT_BUILDER_DATABASE_URL",
    "TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET"
)
$priorTestVariables = @{}
foreach ($name in $testVariableNames) {
    $priorTestVariables[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

Push-Location -LiteralPath $PSScriptRoot
try {
    try {
        $mutexHeld = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $mutexHeld = $true
    }
    if (-not $mutexHeld) {
        throw "another destructive live-database gate is already running"
    }
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is required"
    }
    foreach ($name in @(
        "COMPOSE_FILE",
        "COMPOSE_PROFILES",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_ENV_FILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "ALEMBIC_CONFIG"
    )) {
        if (-not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name, "Process"))) {
            throw "$name must be unset for the local live-database gate"
        }
    }
    $dockerContext = ([string](docker context show)).Trim()
    if ($LASTEXITCODE -ne 0 -or $dockerContext -cne "desktop-linux") {
        throw "Docker must use the local desktop-linux context"
    }
    $dockerEndpoint = ([string](
        docker context inspect "desktop-linux" --format "{{.Endpoints.docker.Host}}"
    )).Trim()
    if ($LASTEXITCODE -ne 0 -or $dockerEndpoint -cne "npipe:////./pipe/dockerDesktopLinuxEngine") {
        throw "Docker must use the local Docker Desktop Linux endpoint"
    }
    $dockerArgs = @("--context", "desktop-linux")
    $dockerIdentity = ([string](
        docker @dockerArgs info --format "{{.Name}}|{{.OperatingSystem}}"
    )).Trim()
    if ($LASTEXITCODE -ne 0 -or $dockerIdentity -cne "docker-desktop|Docker Desktop") {
        throw "the local Docker Desktop Linux daemon is unavailable"
    }
    $composeFile = (Resolve-Path -LiteralPath "docker-compose.yml").Path
    $envFile = (Resolve-Path -LiteralPath ".env").Path
    $composeArgs = @(
        "--env-file", $envFile,
        "--file", $composeFile,
        "--project-directory", $PSScriptRoot,
        "--project-name", "stock-api"
    )

    $runningServices = @(docker @dockerArgs compose @composeArgs ps --status running --services)
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose status check failed"
    }
    $conflictingServices = @(
        $runningServices | Where-Object { $_ -in @("api", "worker", "beat", "snapshot-builder") }
    )
    if ($conflictingServices.Count -ne 0) {
        throw "stop API, worker, beat, and snapshot-builder before the destructive gate"
    }
    $pythonProcessNamePattern = "^(?:py|python(?:w|[0-9]+(?:\.[0-9]+)?)?|celery|uvicorn)(?:\.exe)?$"
    $nativeServices = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $_.ProcessId -ne $PID -and
                $_.Name -match $pythonProcessNamePattern -and
                $_.CommandLine -match "(?i)\b(?:celery|uvicorn)\b"
            }
    )
    if ($nativeServices.Count -ne 0) {
        throw "stop native Celery and uvicorn processes before the destructive gate"
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

    docker @dockerArgs compose @composeArgs up -d timescaledb
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed to start timescaledb"
    }

    Write-Host "waiting for timescaledb health (maximum 5 minutes)..."
    $deadline = (Get-Date).AddMinutes(5)
    do {
        if ((Get-Date) -ge $deadline) {
            docker @dockerArgs compose @composeArgs logs --tail 100 timescaledb
            throw "timed out waiting for timescaledb health"
        }
        Start-Sleep -Seconds 3
        $health = docker @dockerArgs inspect --format "{{.State.Health.Status}}" stockapi-timescaledb 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "stockapi-timescaledb is missing or stopped"
        }
        if ($health -eq "unhealthy") {
            docker @dockerArgs compose @composeArgs logs --tail 100 timescaledb
            throw "timescaledb became unhealthy"
        }
    } while ($health -ne "healthy")

    $ownerPassword = [uri]::EscapeDataString($vars.POSTGRES_PASSWORD)
    $runtimePassword = [uri]::EscapeDataString($vars.POSTGRES_APP_PASSWORD)
    $builderPassword = [uri]::EscapeDataString($vars.POSTGRES_SNAPSHOT_BUILDER_PASSWORD)
    $database = $vars.POSTGRES_DB

    $env:TEST_DATABASE_URL = "postgresql+asyncpg://stockapi_owner:$ownerPassword@127.0.0.1:5432/$database"
    $env:TEST_RUNTIME_DATABASE_URL = "postgresql+asyncpg://stockapi_app:$runtimePassword@127.0.0.1:5432/$database"
    $env:TEST_SNAPSHOT_BUILDER_DATABASE_URL = "postgresql+asyncpg://stockapi_snapshot_builder:$builderPassword@127.0.0.1:5432/$database"
    $env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET = "stockapi-test-only"
    $runningBeforeReset = @(
        docker @dockerArgs compose @composeArgs ps --status running --services
    )
    if ($LASTEXITCODE -ne 0 -or @(
        $runningBeforeReset |
            Where-Object { $_ -in @("api", "worker", "beat", "snapshot-builder") }
    ).Count -ne 0) {
        throw "a conflicting Compose service appeared before the destructive reset"
    }
    $nativeBeforeReset = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $_.ProcessId -ne $PID -and
                $_.Name -match $pythonProcessNamePattern -and
                $_.CommandLine -match "(?i)\b(?:celery|uvicorn)\b"
            }
    )
    if ($nativeBeforeReset.Count -ne 0) {
        throw "a native Celery or uvicorn process appeared before the destructive reset"
    }
    uv run pytest `
        -p no:cacheprovider `
        --basetemp data/pytest-live-gate `
        tests/integration/test_bars_live_gate.py `
        -v
    if ($LASTEXITCODE -ne 0) {
        throw "live integration gate failed"
    }
}
finally {
    foreach ($name in $testVariableNames) {
        $previous = $priorTestVariables[$name]
        if ($null -eq $previous) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        else {
            Set-Item "Env:$name" -Value $previous
        }
    }
    if ($mutexHeld) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
    Pop-Location
}
